package com.tfedorov.dict

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkSqlApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  if (args.size < 2)
    System.exit(0)

  private val dataPath = args.headOption
  private val dictPath = args.tail.headOption
  log.warn("Data dir:" + dataPath)
  log.warn("Dict dir:" + dictPath)

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  import session.implicits._

  private val inputDF = session
    .read
    .parquet(dataPath.get)
    .toDF("hash", "value")

  private val dictionaryDF = session.read.json(dictPath.get).toDF("des", "key")

  private val resultDF = inputDF.join(dictionaryDF).where($"value" === $"key").select("hash", "des")
  resultDF.show
  println("--------------------------")
  resultDF.explain(true)
  println("--------------------------")

  log.warn("*******End*******")
}
