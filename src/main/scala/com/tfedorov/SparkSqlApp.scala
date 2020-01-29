package com.tfedorov

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkSqlApp extends App with Logging {

  log.warn("*******Start*******")
  private val targetPath = args.headOption
  log.warn("Trying to save into " + targetPath)

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  val inputDF = session
    .read
    .parquet(".\\src\\main\\resources\\data\\tr2")
    .toDF("hash", "value")

  //val resultDF = inputDF.sort("value").filter($"value".contains("A"))
  val resultDF = inputDF.groupBy("value").sum()
  resultDF.show
  println("--------------------------")
  resultDF.explain(true)
  println("--------------------------")


  log.warn("*******End*******")
}
