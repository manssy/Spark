name := "SPMshowcase"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.postgresql" % "postgresql" % "42.1.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.1" % "provided",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.1" % "provided"
)

mainClass in assembly := Some("corporate.DMSpark")

assemblyJarName in assembly := "SPM-2.1.jar"

scalacOptions := Seq("-encoding", "cp1251")

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.contains("services") => MergeStrategy.concat
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}