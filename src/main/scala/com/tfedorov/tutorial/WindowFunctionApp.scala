package com.tfedorov.tutorial

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.StdIn

object WindowFunctionApp extends App {

  private case class Values(city: String, name: String, values: Int)

  private val spark =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import spark.implicits._

  private val values = Seq(
    Values("Lviv", "John", 7),
    Values("Lviv", "John", 5),
    Values("Lviv", "Petro", 85),
    Values("Lviv", "Andriy", 100),
    Values("Lviv", "Vasyl", 21),
    Values("Kyiv", "Vasyl", 221),
    Values("Kyiv", "Petro", 220),
    Values("Kyiv", "Ivan", 1)
  )

  private val valuesDS: Dataset[Values] = spark.createDataset(values)

  valuesDS.createTempView("vals")

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  //val actualDS = spark.sql("SELECT * FROM (SELECT *, rank() OVER (PARTITION BY city ORDER BY values DESC) AS rank FROM vals) WHERE rank < 3")
  private val byBucket = Window.partitionBy('city).orderBy('values)
  valuesDS.withColumn("mean", avg("values").over(byBucket)).show

  //valuesDS.withColumn("rank", rank().over(byBucket)).show //.filter($"rank" < 3)
  valuesDS.repartition(4, $"city").rdd.foreachPartition { i =>
    println("*****")
    i.foreach(print)
  }

  val numParts = 4 // the number of partitions
  val partExprs = Seq($"city")

  val partitionIdExpression = pmod(hash(partExprs: _*), lit(numParts))
  val q = valuesDS.withColumn("partitionId", partitionIdExpression)
  q.show
}
