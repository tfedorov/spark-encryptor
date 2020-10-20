package com.tfedorov


import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrameStatFunctions, Dataset, Row, SparkSession}

import scala.io.StdIn

object GameApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  case class Player1Step(field1: Int, field2: Int, field3: Int) {
    def sum: Int = field1 + field2 + field3

    def compare(another: Player1Step): Int = {
      field1.compareTo(another.field1) +
        field2.compareTo(another.field2) +
        field3.compareTo(another.field3)
    }
  }

  def createAllPlayer1(max: Int): Seq[Player1Step] = {
    val possibles: Seq[Int] = 1 to (max - 2)
    for (f1 <- possibles;
         f2 <- possibles;
         f3 <- possibles)
      yield Player1Step(f1, f2, f3)
  }


  private val spark =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import spark.implicits._

  val dsP1: Dataset[Player1Step] = spark.createDataset(createAllPlayer1(20)).filter(_.sum == 20)
  val dsP2: Dataset[Row] = spark.createDataset(createAllPlayer1(20))
    .filter(_.sum == 20)
    .withColumnRenamed("field1", "field1P2")
    .withColumnRenamed("field2", "field2P2")
    .withColumnRenamed("field3", "field3P2")

  import org.apache.spark.sql.functions._

  val gamesDF = dsP1.crossJoin(dsP2).withColumn("result", signum($"field1" - $"field1P2") + signum($"field2" - $"field2P2") + signum($"field3" - $"field3P2"))
  gamesDF.show
  gamesDF.explain()

  gamesDF.createOrReplaceTempView("game")

  //val gameGroupedDF = spark.sql("SELECT result, field1, count(*) AS numOfGames FROM game WHERE result > 0 GROUP BY result, field1 ORDER BY numOfGames DESC")
  val gameGroupedDF = spark.sql("SELECT result, field1, field2, field3, count(*) AS numOfGames FROM game WHERE result > 0 GROUP BY result, field1, field2, field3  ORDER BY numOfGames DESC")
  gameGroupedDF.show

   gameGroupedDF.summary().show()
  //gamesDF.where($"field1" === 7).where($"field2" === 6).where($"field3"===7).agg(sum("result")).show
  //gameGroupedDF.explain()
  println("Press RETURN to stop...")
  StdIn.readLine()
  log.warn("*******End*******")
}
