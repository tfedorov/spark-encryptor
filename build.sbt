import sbt.Keys._
import sbt._

name := "spark_encryptor"

version := "0.1"

scalaVersion := "2.12.3"

val sparkExcludeLibs = sys.env.getOrElse("SPARK_EXCLUDE_LIBS", "false")

val sparkDependencies: Seq[ModuleID] = sparkExcludeLibs match {
  case "true" =>
    Seq("org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1" % "provided")

  case _ =>
    Seq("org.apache.spark" %% "spark-core" % "3.0.1",
      "org.apache.spark" %% "spark-sql" % "3.0.1",
      "org.apache.spark" %% "spark-streaming" % "3.0.1" ,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1")
}

libraryDependencies ++= sparkDependencies
libraryDependencies += "org.postgresql" % "postgresql" % "9.2-1002-jdbc4"
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-api" % "5.7.0-RC1" % Test

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}


lazy val manifestSettings = Seq(
  packageOptions in(Compile, packageBin) +=
    Package.ManifestAttributes(
      "git_last_commit" -> git.gitHeadCommit.value.toString,
      "git_last_message" -> git.gitHeadMessage.value.toString.replaceAll("\n", ""))
)

lazy val root = Project(id = "root", base = file(".")).settings(manifestSettings: _*)