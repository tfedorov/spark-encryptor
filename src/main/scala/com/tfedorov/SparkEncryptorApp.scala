package com.tfedorov

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkEncryptorApp extends App with Logging {
  log.info("Start")

  private val session = SparkSession.builder.
    master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val sc = session.sparkContext

  private val md = MessageDigest.getInstance("MD5")

  private val input = (' ' to '~').map(_.toString)

  val inputRDD = sc.parallelize(input)

  val keysRDD = inputRDD.cartesian(inputRDD).map(t => t._1 + t._2)
    .cartesian(inputRDD).map(t => t._1 + t._2)
    //.cartesian(inputRDD).map(t => t._1 + t._2)
    //.cartesian(inputRDD).map(t => t._1 + t._2)
    //.cartesian(inputRDD).map(t => t._1 + t._2)

  println(keysRDD.count())

  val result = keysRDD.map { key =>
    val messageDigest = md.digest(key.getBytes())
    val no = new BigInteger(1, messageDigest)
    val hashtext = no.toString(16)
    (hashtext, key)
  }.sortByKey().count() ///.take(100).foreach(println)

  println(result)

  log.info("End")
}
