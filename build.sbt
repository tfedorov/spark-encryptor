name := "spark_encryptor"

version := "0.1"

scalaVersion := "2.11.8"

val sparkLocal = sys.env.getOrElse("SPARK_LOCAL", "false")

val sparkDependencies: Seq[ModuleID] = sparkLocal match {
  case "false" =>
    Seq("org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided")

  case _ =>
    Seq("org.apache.spark" %% "spark-core" % "2.4.4",
      "org.apache.spark" %% "spark-sql" % "2.4.4")

}

libraryDependencies ++= sparkDependencies