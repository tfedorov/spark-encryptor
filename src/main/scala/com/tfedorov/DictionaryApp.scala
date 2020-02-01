package com.tfedorov

import java.security.MessageDigest

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DictionaryApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")
  private val targetPath = args.headOption
  log.warn("Trying to save into " + targetPath)
  if (targetPath.isEmpty)
    System.exit(0)

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val sc = session.sparkContext

  import session.implicits._

  private val md = MessageDigest.getInstance("MD5")

  private val input = Seq(("abc", "cool"), ("cba", "zeba"), ("ccc", "clab"), ("aac", "coal"), ("caa", "zeaa"), ("cac", "caab"))

  private val inputRDD: RDD[(String, String)] = sc.parallelize(input)

  inputRDD.toDF("key", "des").write.json(targetPath.get)
  log.warn("*******End*******")
}
