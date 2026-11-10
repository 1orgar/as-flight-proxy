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
 */
object ZstdArrowCodec extends AbstractCompressionCodec {
  private val DefaultCompressionLevel: Int = 1

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
      DefaultCompressionLevel
    )
    if (Zstd.isError(bytesWritten)) {
      compressedBuffer.close()
      throw new RuntimeException(
        s"Arrow IPC ZSTD compression failed: ${Zstd.getErrorName(bytesWritten)}"
      )
    }

    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + bytesWritten)
    compressedBuffer
  }

  override protected def doDecompress(
      allocator: BufferAllocator,
      compressedBuffer: ArrowBuf
  ): ArrowBuf = {
    val decompressedLength = readUncompressedLength(compressedBuffer)
    val uncompressedBuffer = allocator.buffer(decompressedLength)
    if (decompressedLength == 0L) {
      return uncompressedBuffer
    }

    val compressedOffset =
      compressedBuffer.readerIndex() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
    val compressedSize =
      compressedBuffer.readableBytes() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH

    val srcAddr = compressedBuffer.memoryAddress() + compressedOffset
    val dstAddr = uncompressedBuffer.memoryAddress()

    val decompressedBytes =
      Zstd.decompressUnsafe(dstAddr, decompressedLength, srcAddr, compressedSize)
    if (Zstd.isError(decompressedBytes)) {
      uncompressedBuffer.close()
      throw new RuntimeException(
        s"Arrow IPC ZSTD decompression failed: ${Zstd.getErrorName(decompressedBytes)}"
      )
    }

    uncompressedBuffer.writerIndex(decompressedBytes)
    uncompressedBuffer
  }
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
    val uncompressedSize = uncompressedBuffer.readableBytes().toInt
    val rawBytes = new Array[Byte](uncompressedSize)
    uncompressedBuffer.getBytes(uncompressedBuffer.readerIndex(), rawBytes)

    val baos = new ByteArrayOutputStream( math.max(64, uncompressedSize / 2) )
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
    val decompressedLength = readUncompressedLength(compressedBuffer).toInt
    val uncompressedBuffer = allocator.buffer(decompressedLength)
    if (decompressedLength == 0) {
      return uncompressedBuffer
    }

    val compressedOffset =
      compressedBuffer.readerIndex() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
    val compressedSize =
      (compressedBuffer.readableBytes() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH).toInt

    val compressedBytes = new Array[Byte](compressedSize)
    compressedBuffer.getBytes(compressedOffset, compressedBytes)

    val lz4In = new LZ4FrameInputStream(new ByteArrayInputStream(compressedBytes))
    val outBytes = new Array[Byte](decompressedLength)
    var totalRead = 0
    while (totalRead < decompressedLength) {
      val read = lz4In.read(outBytes, totalRead, decompressedLength - totalRead)
      if (read < 0) {
        uncompressedBuffer.close()
        lz4In.close()
        throw new RuntimeException(
          s"Arrow IPC LZ4_FRAME truncated stream: expected $decompressedLength bytes, got $totalRead"
        )
      }
      totalRead += read
    }
    lz4In.close()

    uncompressedBuffer.setBytes(0, outBytes, 0, decompressedLength)
    uncompressedBuffer.writerIndex(decompressedLength)
    uncompressedBuffer
  }
}

/**
 * Arrow `CompressionCodec.Factory` supporting `ZSTD`, `LZ4_FRAME`, and uncompressed buffers.
 */
object ArrowCompressionCodecFactory extends CompressionCodec.Factory {
  override def createCodec(codecType: CompressionUtil.CodecType): CompressionCodec = {
    codecType match {
      case CompressionUtil.CodecType.ZSTD      => ZstdArrowCodec
      case CompressionUtil.CodecType.LZ4_FRAME => Lz4FrameArrowCodec
      case _ => NoCompressionCodec.Factory.INSTANCE.createCodec(codecType)
    }
  }

  override def createCodec(
      codecType: CompressionUtil.CodecType,
      compressionLevel: Int
  ): CompressionCodec = {
    createCodec(codecType)
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
   */
  def normalizeCompression(raw: String, enabled: Boolean = true): String = {
    if (!enabled) {
      return "none"
    }
    raw.trim.toLowerCase(Locale.ROOT) match {
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
      compression: String
  ): Unit = {
    listener.setUseZeroCopy(true)
    val codecOpt: Option[CompressionCodec] = compression match {
      case "zstd" => Some(ZstdArrowCodec)
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
