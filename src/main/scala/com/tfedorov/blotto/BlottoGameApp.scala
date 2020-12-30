package com.tfedorov.blotto

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object BlottoGameApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getSimpleName + "*******")

  private final val NUMBER_OF_CHIPS = 25

  private val spark =
    SparkSession.builder //.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import spark.implicits._

  val player1DS: Dataset[PlayerStep] = spark.createDataset(PlayerStep.generateAll(NUMBER_OF_CHIPS))
    .filter(_.sum == NUMBER_OF_CHIPS)
    .as("player1")
  val player2DS: Dataset[PlayerStep] = spark.createDataset(PlayerStep.generateAll(NUMBER_OF_CHIPS))
    .filter(_.sum == NUMBER_OF_CHIPS)
    .as("player2")

  import org.apache.spark.sql.functions._

  val gamesDF = player1DS.crossJoin(player2DS)
    .withColumn("result",
      signum($"player1.field1" - $"player2.field1") + signum($"player1.field2" - $"player2.field2") + signum($"player1.field3" - $"player2.field3") + signum($"player1.field4" - $"player2.field4"))
  //.filter($"result" === 1)

  val gamesGrouped = gamesDF.groupBy("player1.field1", "player1.field2", "player1.field3", "player1.field4")
    .agg(count("*").as("all"), sum("result").as("general"))
    .orderBy($"general".desc)
  gamesGrouped.explain(true)

  gamesGrouped.show()

  log.warn("*******End*******")
}
