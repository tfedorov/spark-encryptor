import sbt.Keys._
import sbt._

name := "spark_encryptor"

version := "0.1"

scalaVersion := "2.12.3"

val sparkExcludeLibs = sys.env.getOrElse("SPARK_EXCLUDE_LIBS", "false")

val sparkDependencies: Seq[ModuleID] = sparkExcludeLibs match {
  case "true" =>
    Seq("org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided")

  case _ =>
    Seq("org.apache.spark" %% "spark-core" % "2.4.4",
      "org.apache.spark" %% "spark-sql" % "2.4.4")

}

libraryDependencies ++= sparkDependencies
libraryDependencies += "org.postgresql" % "postgresql" % "9.2-1002-jdbc4"

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