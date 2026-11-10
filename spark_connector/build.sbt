name := "spark-aerospike-flight-connector"
organization := "com.asproxy"
version := "0.4.2"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.2"
val arrowVersion = "15.0.2"

libraryDependencies ++= Seq(
  // Spark dependencies provided by the Hadoop / Spark cluster runtime
  "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

  // Apache Arrow Flight & Vector dependencies bundled into the fat-JAR
  "org.apache.arrow" % "flight-core"        % arrowVersion,
  "org.apache.arrow" % "flight-grpc"        % arrowVersion,
  "org.apache.arrow" % "arrow-vector"       % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

// Align Jackson version with Spark 3.5.2's jackson-module-scala 2.15.2
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core"        % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-databind"    % "2.15.2"
)

coverageMinimumStmtTotal := 85
coverageFailOnMinimum := true

// Include "provided" dependencies (spark-sql, spark-catalyst) on the Test runtime classpath
Test / fullClasspath := (Test / fullClasspath).value ++ (Compile / managedClasspath).value

// Shading rules to isolate gRPC, Protobuf, Guava, and Arrow from Hadoop / Spark 3.5 classpath conflicts
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("io.grpc.**" -> "com.asproxy.shaded.io.grpc.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "com.asproxy.shaded.com.google.protobuf.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "com.asproxy.shaded.com.google.common.@1").inAll,
  ShadeRule.rename("org.apache.arrow.**" -> "com.asproxy.shaded.org.apache.arrow.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) =>
    xs map { _.toLowerCase } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
        MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case "module-info.class"    => MergeStrategy.discard
  case "git.properties"       => MergeStrategy.discard
  case "arrow-git.properties" => MergeStrategy.discard
  case x if x.endsWith("DefaultAllocationManagerFactory.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
