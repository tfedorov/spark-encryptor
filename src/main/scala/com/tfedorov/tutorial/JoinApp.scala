package com.tfedorov.tutorial

import com.tfedorov.utils.FilesUtils
import org.apache.spark.sql.SparkSession

object JoinApp extends App {

  private val localEnv = sys.env.getOrElse("SPARK_LOCAL", "false")
  println("SPARK_LOCAL=" + localEnv)
  println("*********")

  println(FilesUtils.readAssemblyManifest())
  println("*********")

  private val session: SparkSession = if ("true".equalsIgnoreCase(localEnv))
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()
  else
    SparkSession.builder
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()


  case class TableA(x: Int, y: Int, valueA: String)

  case class TableB(x: Int, y: Int, valueB: String)


  import session.sqlContext.implicits._

  val dfA = session.createDataset(TableA(1, 2, "Sabbra") :: TableA(3, 4, "Zuba") :: Nil)
  val dfB = session.createDataset(TableB(1, 2, "Cadabra") :: TableB(5, 6, "Bubra") :: Nil)
  dfA.createTempView("A")
  dfB.createTempView("B")
  //SizeEstimator.estimate(TableA(1, 2, "Sabbra"))
  //dfB.printSchema()
  dfA.as("A").join(dfB.as("B"), $"A.x" === $"B.x").show
  session.sql("SELECT * FROM A JOIN B ON A.x = B.x ").show
  session.sql("SELECT * FROM A JOIN B ON A.x == B.x ").show
  dfA.join(dfB, Seq("x")).show
  dfA.join(dfB, Seq("x"), "fullouter").show


  //session.sql("SELECT * FROM A LEFT ANTI JOIN B ON A.x == B.x").show


}
