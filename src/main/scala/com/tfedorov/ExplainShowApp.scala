package com.tfedorov


import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object ExplainShowApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  private val spark =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  val t1 = spark.range(5)
  val t2 = spark.range(5)
  //val q = t1.join(t2).where(t1("id") === t2("id"))
  import spark.implicits._
  spark.sqlContext.setConf("spark.sql.crossJoin.enabled","true")
  val q = t1.join(t2)//.where(t1("id") === t2("id"))
  q.explain()
  q.show()
  println("Press RETURN to stop...")
  StdIn.readLine()
  log.warn("*******End*******")
}
