package com.tfedorov.blotto

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object BlottoGameSqlApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")
  private final val NUMBER_OF_CHIPS = 25

  private val spark =
    SparkSession.builder//.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import spark.implicits._

  val player1DS: Dataset[PlayerStep] = spark.createDataset(PlayerStep.generateAll(NUMBER_OF_CHIPS))
    .filter(_.sum == NUMBER_OF_CHIPS)
  player1DS.createOrReplaceTempView("player1")

  val player2DS: Dataset[PlayerStep] = spark.createDataset(PlayerStep.generateAll(NUMBER_OF_CHIPS))
    .filter(_.sum == NUMBER_OF_CHIPS)
  player2DS.createOrReplaceTempView("player2")

  val gamesDF = spark.sql(
    """SELECT
      |  player1.field1 P1F1,
      |  player1.field2 P1F2,
      |  player1.field3 P1F3,
      |  player1.field4 P1F4,
      |  player2.field1 P2F1,
      |  player2.field2 P2F2,
      |  player2.field3 P2F3,
      |  player2.field4 P2F4,
      |  signum(player1.field1 - player2.field1) + signum(player1.field2 - player2.field2) + signum(player1.field3 - player2.field3) + signum(player1.field4 - player2.field4) result
      |FROM
      |  player1 CROSS
      |  JOIN player2
      |ORDER BY result DESC""".stripMargin)

  gamesDF.createOrReplaceTempView("game")

  //  (case when result > 0.0 then 'win' when result < 0.0 then 'lost' else 'draft' end) AS gameResult,
  val groupSQL =
    """SELECT
      |  P1F1,
      |  P1F2,
      |  P1F3,
      |  P1F4,
      |  count(*) AS all,
      |  sum(result) AS general
      |FROM
      |  game
      |GROUP BY
      |  P1F1,
      |  P1F2,
      |  P1F3,
      |  P1F4
      |ORDER BY general DESC
      |""".stripMargin
  val gameGroupedDF = spark.sql(groupSQL)

  gameGroupedDF.explain(true)
  gameGroupedDF.summary().show()
  gameGroupedDF.show

  log.warn("*******End*******")
}
