package com.tfedorov

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkEncryptorApp extends App with Logging {

  log.warn("*******Start*******")
  private val targetPath = args.headOption
  log.warn("Trying to save into " + targetPath)

  private val session = SparkSession.builder//.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val sc = session.sparkContext

  private val md = MessageDigest.getInstance("MD5")

  private val input = (' ' to '~').map(_.toString)

  private val inputRDD = sc.parallelize(input)

  private val keysRDD = inputRDD.cartesian(inputRDD).map(t => t._1 + t._2)
    .cartesian(inputRDD).map(t => t._1 + t._2)
  //.cartesian(inputRDD).map(t => t._1 + t._2)
  //.cartesian(inputRDD).map(t => t._1 + t._2)
  //.cartesian(inputRDD).map(t => t._1 + t._2)

  //println(keysRDD.count())

  private val resultRDD = keysRDD.map { key =>
    val messageDigest = md.digest(key.getBytes())
    val no = new BigInteger(1, messageDigest)
    val hashText = no.toString(16)
    (hashText, key)
  }.sortByKey()

  targetPath
    .map(path => resultRDD.saveAsTextFile(path))
    .getOrElse(resultRDD.take(100).foreach(println))

  log.warn("*******End*******")
}
