package com.tfedorov

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object PostgresDriverApp extends App with Logging {

  log.warn("*******Start*******")
  private val targetPath = args.headOption
  log.warn("Trying to save into " + targetPath)
  if (targetPath.isEmpty)
    System.exit(0)

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val input: DataFrame =  session.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost/tmp?user=postgres&password=Ckfdf1")
    .option("query", "select hash, value from tmp.tmp_table")
    .load()

  input.show
}
