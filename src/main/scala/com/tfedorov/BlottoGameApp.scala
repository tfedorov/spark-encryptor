package com.tfedorov


import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object BlottoGameApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  case class Player1Step(field1: Int, field2: Int, field3: Int, field4: Int) {
    def sum: Int = field1 + field2 + field3 + field4

    def compare(another: Player1Step): Int = {
      field1.compareTo(another.field1) +
        field2.compareTo(another.field2) +
        field3.compareTo(another.field3) +
        field4.compareTo(another.field4)
    }
  }

  def createAllPlayer1(max: Int): Seq[Player1Step] = {
    val possibles: Seq[Int] = 1 to (max - 2)
    for (f1 <- possibles;
         f2 <- possibles;
         f3 <- possibles;
         f4 <- possibles)
      yield Player1Step(f1, f2, f3, f4)
  }

  private val spark =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import spark.implicits._

  private final val NUMBER_OF_CHIPS = 25

  val player1DS: Dataset[Player1Step] = spark.createDataset(createAllPlayer1(NUMBER_OF_CHIPS)).filter(_.sum == NUMBER_OF_CHIPS)
  val player2DS: Dataset[Row] = spark.createDataset(createAllPlayer1(NUMBER_OF_CHIPS))
    .filter(_.sum == NUMBER_OF_CHIPS)
    .withColumnRenamed("field1", "field1P2")
    .withColumnRenamed("field2", "field2P2")
    .withColumnRenamed("field3", "field3P2")
    .withColumnRenamed("field4", "field4P2")

  import org.apache.spark.sql.functions._

  val gamesDF = player1DS.crossJoin(player2DS)
    .withColumn("result", signum($"field1" - $"field1P2") + signum($"field2" - $"field2P2") + signum($"field3" - $"field3P2") + signum($"field4" - $"field4P2"))
  //gamesDF.show
  //gamesDF.explain(true)

  gamesDF.createOrReplaceTempView("game")

  //val gameGroupedDF = spark.sql("SELECT result, field1, count(*) AS numOfGames FROM game WHERE result > 0 GROUP BY result, field1 ORDER BY numOfGames DESC")
  val groupSQL =
    """SELECT (case when result >0.0 then 'win' when result < 0.0 then 'lost' else 'draft' end) AS gameResult, field1, field2, field3, field4, count(*) AS numOfGames
      |  FROM game
      |WHERE result > 0
      |GROUP BY result, field1, field2, field3, field4
      |ORDER BY numOfGames DESC""".stripMargin
  val gameGroupedDF = spark.sql(groupSQL)
  gameGroupedDF.show

  //gameGroupedDF.summary().show()
  //gamesDF.where($"field1" === 7).where($"field2" === 6).where($"field3"===7).agg(sum("result")).show
  //gameGroupedDF.explain(true)

  log.warn("*******End*******")
}
