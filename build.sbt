import AssemblyKeys._

name := "Spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11"  % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.11"  % "2.0.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11"  % "2.0.0",
//  "org.apache.kafka" % "kafka_2.11"  % "0.10.0.1",
  "org.apache.spark" % "spark-mllib_2.11"% "2.0.0"
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
