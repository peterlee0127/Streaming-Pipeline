import sbtassembly.Plugin.AssemblyKeys._

name := "Spark"

version := "1.0"

scalaVersion := "2.11.8"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

//lazy val root = (project in file(".")).enablePlugins(PlayScala)

libraryDependencies ++= Seq(
//  "com.typesafe.play" %% "play-json" % "2.5.10",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11"  % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.11"  % "2.0.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11"  % "2.0.0",
//  "org.apache.kafka" % "kafka_2.11"  % "0.10.0.1",
  "org.apache.spark" % "spark-mllib_2.11"% "2.0.0"
//  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.5"
)

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
