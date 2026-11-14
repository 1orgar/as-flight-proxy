package com.asproxy.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.reflect.Field
import java.util.Locale
import java.util.concurrent.{Executor, Future}

import com.github.luben.zstd.Zstd
import io.grpc.stub.AbstractStub
import net.jpountz.lz4.{LZ4FrameInputStream, LZ4FrameOutputStream}
import org.apache.arrow.flight.{CallOption, CallOptions, FlightStream, OutboundStreamListener}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.{
  AbstractCompressionCodec,
  CompressionCodec,
  CompressionUtil,
  NoCompressionCodec
}

/**
 * Off-heap ZSTD Arrow IPC buffer compression codec backed by `zstd-jni` (bundled with Apache Spark).
 * Operates directly on native `ArrowBuf.memoryAddress()` pointers with zero heap copies.
 *
 * Supported ZSTD compression levels:
 *   - `1..9`: standard range (`1` = fastest / default, `3` = ZSTD default balance, `9` = high ratio)
 *   - `10..22`: ultra compression levels (higher CPU/memory cost)
 *   - `-7..-1`: negative fast levels (higher speed, lower ratio)
 */
class ZstdArrowCodec(val compressionLevel: Int = ZstdArrowCodec.DefaultCompressionLevel)
    extends AbstractCompressionCodec {
  require(
    compressionLevel >= ZstdArrowCodec.MinCompressionLevel &&
      compressionLevel <= ZstdArrowCodec.MaxCompressionLevel,
    s"ZSTD compression level must be between ${ZstdArrowCodec.MinCompressionLevel} and ${ZstdArrowCodec.MaxCompressionLevel} (standard range: 1..9, got $compressionLevel)"
  )

  override def getCodecType: CompressionUtil.CodecType = CompressionUtil.CodecType.ZSTD

  override protected def doCompress(
      allocator: BufferAllocator,
      uncompressedBuffer: ArrowBuf
  ): ArrowBuf = {
    val uncompressedSize = uncompressedBuffer.readableBytes()
    val maxCompressedSize = Zstd.compressBound(uncompressedSize)
    val compressedBuffer =
      allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + maxCompressedSize)

    val srcAddr = uncompressedBuffer.memoryAddress() + uncompressedBuffer.readerIndex()
    val dstAddr = compressedBuffer.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH

    val bytesWritten = Zstd.compressUnsafe(
      dstAddr,
      maxCompressedSize,
      srcAddr,
      uncompressedSize,
      compressionLevel
    )
    if (Zstd.isError(bytesWritten)) {
      compressedBuffer.close()
      throw new RuntimeException(
        s"Arrow IPC ZSTD compression failed (level=$compressionLevel): ${Zstd.getErrorName(bytesWritten)}"
      )
    }

    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + bytesWritten)
    compressedBuffer
  }

  override protected def doDecompress(
      allocator: BufferAllocator,
      compressedBuffer: ArrowBuf
  ): ArrowBuf = {
    if (compressedBuffer.readableBytes() < CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH) {
      throw new IllegalArgumentException(
        s"Arrow IPC ZSTD buffer too short (${compressedBuffer.readableBytes()} bytes)"
      )
    }
    val decompressedLength = readUncompressedLength(compressedBuffer)
    if (decompressedLength < 0L || decompressedLength > Int.MaxValue.toLong) {
      throw new IllegalArgumentException(
        s"Invalid Arrow IPC ZSTD uncompressed length: $decompressedLength"
      )
    }
    val uncompressedBuffer = allocator.buffer(decompressedLength)
    if (decompressedLength == 0L) {
      return uncompressedBuffer
    }

    try {
      val compressedOffset =
        compressedBuffer.readerIndex() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
      val compressedSize =
        compressedBuffer.readableBytes() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH

      val srcAddr = compressedBuffer.memoryAddress() + compressedOffset
      val dstAddr = uncompressedBuffer.memoryAddress()

      val decompressedBytes =
        Zstd.decompressUnsafe(dstAddr, decompressedLength, srcAddr, compressedSize)
      if (Zstd.isError(decompressedBytes)) {
        throw new RuntimeException(
          s"Arrow IPC ZSTD decompression failed: ${Zstd.getErrorName(decompressedBytes)}"
        )
      }
      if (decompressedBytes != decompressedLength) {
        throw new RuntimeException(
          s"Arrow IPC ZSTD uncompressed size mismatch: expected $decompressedLength bytes, got $decompressedBytes"
        )
      }

      uncompressedBuffer.writerIndex(decompressedBytes)
      uncompressedBuffer
    } catch {
      case t: Throwable =>
        uncompressedBuffer.close()
        throw t
    }
  }
}

object ZstdArrowCodec {
  final val DefaultCompressionLevel: Int = 1
  final val MinCompressionLevel: Int = -7
  final val MaxCompressionLevel: Int = 22
  lazy val DefaultInstance: ZstdArrowCodec = new ZstdArrowCodec(DefaultCompressionLevel)
}

/**
 * Arrow IPC `LZ4_FRAME` buffer compression codec backed by `lz4-java` (bundled with Apache Spark).
 */
object Lz4FrameArrowCodec extends AbstractCompressionCodec {
  override def getCodecType: CompressionUtil.CodecType = CompressionUtil.CodecType.LZ4_FRAME

  override protected def doCompress(
      allocator: BufferAllocator,
      uncompressedBuffer: ArrowBuf
  ): ArrowBuf = {
    val rawSize = uncompressedBuffer.readableBytes()
    require(
      rawSize >= 0L && rawSize <= Int.MaxValue.toLong,
      s"Arrow IPC LZ4_FRAME uncompressed buffer size out of range: $rawSize"
    )
    val uncompressedSize = rawSize.toInt
    val rawBytes = new Array[Byte](uncompressedSize)
    uncompressedBuffer.getBytes(uncompressedBuffer.readerIndex(), rawBytes)

    val baos = new ByteArrayOutputStream(math.max(64, uncompressedSize / 2))
    val lz4Out = new LZ4FrameOutputStream(baos)
    lz4Out.write(rawBytes)
    lz4Out.close()
    val compressedBytes = baos.toByteArray

    val compressedBuffer =
      allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + compressedBytes.length)
    compressedBuffer.setBytes(
      CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
      compressedBytes,
      0,
      compressedBytes.length
    )
    compressedBuffer.writerIndex(
      CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + compressedBytes.length
    )
    compressedBuffer
  }

  override protected def doDecompress(
      allocator: BufferAllocator,
      compressedBuffer: ArrowBuf
  ): ArrowBuf = {
    if (compressedBuffer.readableBytes() < CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH) {
      throw new IllegalArgumentException(
        s"Arrow IPC LZ4_FRAME buffer too short (${compressedBuffer.readableBytes()} bytes)"
      )
    }
    val rawDecompressedLength = readUncompressedLength(compressedBuffer)
    if (rawDecompressedLength < 0L || rawDecompressedLength > Int.MaxValue.toLong) {
      throw new IllegalArgumentException(
        s"Invalid Arrow IPC LZ4_FRAME uncompressed length: $rawDecompressedLength"
      )
    }
    val decompressedLength = rawDecompressedLength.toInt
    val uncompressedBuffer = allocator.buffer(decompressedLength)
    if (decompressedLength == 0) {
      return uncompressedBuffer
    }

    var lz4In: LZ4FrameInputStream = null
    try {
      val compressedOffset =
        compressedBuffer.readerIndex() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
      val compressedSize =
        (compressedBuffer.readableBytes() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH).toInt

      val compressedBytes = new Array[Byte](compressedSize)
      compressedBuffer.getBytes(compressedOffset, compressedBytes)

      lz4In = new LZ4FrameInputStream(new ByteArrayInputStream(compressedBytes))
      val outBytes = new Array[Byte](decompressedLength)
      var totalRead = 0
      while (totalRead < decompressedLength) {
        val read = lz4In.read(outBytes, totalRead, decompressedLength - totalRead)
        if (read < 0) {
          throw new RuntimeException(
            s"Arrow IPC LZ4_FRAME truncated stream: expected $decompressedLength bytes, got $totalRead"
          )
        }
        totalRead += read
      }

      uncompressedBuffer.setBytes(0, outBytes, 0, decompressedLength)
      uncompressedBuffer.writerIndex(decompressedLength)
      uncompressedBuffer
    } catch {
      case t: Throwable =>
        uncompressedBuffer.close()
        throw t
    } finally {
      if (lz4In != null) {
        try { lz4In.close() } catch { case _: Throwable => }
      }
    }
  }
}

/**
 * Arrow `CompressionCodec.Factory` supporting `ZSTD`, `LZ4_FRAME`, and uncompressed buffers.
 */
object ArrowCompressionCodecFactory extends CompressionCodec.Factory {
  override def createCodec(codecType: CompressionUtil.CodecType): CompressionCodec = {
    codecType match {
      case CompressionUtil.CodecType.ZSTD      => ZstdArrowCodec.DefaultInstance
      case CompressionUtil.CodecType.LZ4_FRAME => Lz4FrameArrowCodec
      case _ => NoCompressionCodec.Factory.INSTANCE.createCodec(codecType)
    }
  }

  override def createCodec(
      codecType: CompressionUtil.CodecType,
      compressionLevel: Int
  ): CompressionCodec = {
    codecType match {
      case CompressionUtil.CodecType.ZSTD      => new ZstdArrowCodec(compressionLevel)
      case CompressionUtil.CodecType.LZ4_FRAME => Lz4FrameArrowCodec
      case _ => NoCompressionCodec.Factory.INSTANCE.createCodec(codecType)
    }
  }
}

/**
 * Helper for configuring Arrow Flight IPC buffer compression (`zstd`, `lz4`),
 * gRPC transport compression (`gzip`), zero-copy protobuf serialization, and disableable compression (`none`).
 */
object FlightCompressionHelper {
  private val DirectExecutor: Executor = new Executor {
    override def execute(command: Runnable): Unit = command.run()
  }

  private lazy val flightStreamRootField: Field = {
    val f = classOf[FlightStream].getDeclaredField("root")
    f.setAccessible(true)
    f
  }

  private lazy val flightStreamLoaderField: Field = {
    val f = classOf[FlightStream].getDeclaredField("loader")
    f.setAccessible(true)
    f
  }

  /**
   * Normalizes user-specified compression options into `"zstd"`, `"lz4"`, `"gzip"`, or `"none"`.
   * Also supports inline level syntax such as `"zstd:3"`.
   */
  def normalizeCompression(raw: String, enabled: Boolean = true): String = {
    if (!enabled) {
      return "none"
    }
    val base = raw.trim.toLowerCase(Locale.ROOT).split(":", 2).head.trim
    base match {
      case "" | "none" | "off" | "false" | "disabled" | "0" => "none"
      case "zstd" | "zstandard" | "true" | "1"              => "zstd"
      case "lz4" | "lz4_frame" | "lz4-frame" | "lz4frame"   => "lz4"
      case "gzip" | "gz"                                    => "gzip"
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported flight.compression '$other'. Expected 'zstd', 'lz4', 'gzip', or 'none'"
        )
    }
  }

  /**
   * Parses the ZSTD compression level from an explicit `flight.compression.level` option or
   * an inline `"zstd:<level>"` suffix, defaulting to `1` (fast).
   * Valid range: `-7..22` (standard range: `1..9`).
   */
  def parseCompressionLevel(rawCompression: String, explicitLevelOpt: Option[String]): Int = {
    val inlineLevelOpt = Option(rawCompression)
      .map(_.trim)
      .filter(_.contains(":"))
      .map(_.split(":", 2)(1).trim)
      .filter(_.nonEmpty)

    val levelStrOpt = explicitLevelOpt.map(_.trim).filter(_.nonEmpty).orElse(inlineLevelOpt)
    val level = levelStrOpt match {
      case Some(s) =>
        try {
          s.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid flight.compression.level '$s'. Expected integer in ${ZstdArrowCodec.MinCompressionLevel}..${ZstdArrowCodec.MaxCompressionLevel} (standard: 1..9)"
            )
        }
      case None => ZstdArrowCodec.DefaultCompressionLevel
    }

    if (level < ZstdArrowCodec.MinCompressionLevel || level > ZstdArrowCodec.MaxCompressionLevel) {
      throw new IllegalArgumentException(
        s"Invalid flight.compression.level '$level'. Expected integer between ${ZstdArrowCodec.MinCompressionLevel} and ${ZstdArrowCodec.MaxCompressionLevel} (standard range: 1..9)"
      )
    }
    level
  }

  /**
   * Hooks `FlightStream` so that as soon as the schema message arrives and `fulfilledRoot` is created,
   * `FlightStream.loader` is replaced with a `VectorLoader` backed by `ArrowCompressionCodecFactory`.
   * This allows transparent decoding of `ZSTD`, `LZ4_FRAME`, and uncompressed Arrow Flight batches.
   */
  def enableStreamDecompression(stream: FlightStream): Unit = {
    val rootFutureObj = flightStreamRootField.get(stream)
    val installLoader = new Runnable {
      override def run(): Unit = {
        val future = rootFutureObj.asInstanceOf[Future[VectorSchemaRoot]]
        if (future.isDone && !future.isCancelled) {
          val root = future.get()
          if (root != null) {
            flightStreamLoaderField.set(
              stream,
              new VectorLoader(root, ArrowCompressionCodecFactory)
            )
          }
        }
      }
    }

    // Guava ListenableFuture.addListener(Runnable, Executor) executes synchronously on DirectExecutor
    // right inside FlightStream$Observer.onNext(SCHEMA) when `this.root.set(this.fulfilledRoot)` is called.
    val addListenerMethod =
      rootFutureObj.getClass.getMethod("addListener", classOf[Runnable], classOf[Executor])
    addListenerMethod.setAccessible(true)
    addListenerMethod.invoke(rootFutureObj, installLoader, DirectExecutor)
  }

  /**
   * Enables Arrow Flight zero-copy writes and installs an IPC buffer compression `VectorUnloader`
   * when `compression` is `"zstd"` or `"lz4"`.
   */
  def configureOutboundStream(
      listener: OutboundStreamListener,
      root: VectorSchemaRoot,
      compression: String,
      compressionLevel: Int = ZstdArrowCodec.DefaultCompressionLevel
  ): Unit = {
    listener.setUseZeroCopy(true)
    val codecOpt: Option[CompressionCodec] = compression match {
      case "zstd" => Some(new ZstdArrowCodec(compressionLevel))
      case "lz4"  => Some(Lz4FrameArrowCodec)
      case _      => None
    }
    codecOpt.foreach { codec =>
      val unloader = new VectorUnloader(root, true, codec, true)
      findFieldInHierarchy(listener.getClass, "unloader").foreach { field =>
        field.setAccessible(true)
        field.set(listener, unloader)
      }
    }
  }

  /**
   * Returns a gRPC `CallOption` enabling transport-level `gzip` compression when `compression == "gzip"`.
   */
  def grpcCompressionCallOption(compression: String): Option[CallOption] = {
    if (compression == "gzip") {
      Some(new CallOptions.GrpcCallOption {
        override def wrapStub[T <: AbstractStub[T]](stub: T): T = {
          stub.withCompression("gzip")
        }
      })
    } else {
      None
    }
  }

  private def findFieldInHierarchy(cls: Class[_], fieldName: String): Option[Field] = {
    var current: Class[_] = cls
    while (current != null) {
      try {
        return Some(current.getDeclaredField(fieldName))
      } catch {
        case _: NoSuchFieldException =>
          current = current.getSuperclass
      }
    }
    None
  }
}
