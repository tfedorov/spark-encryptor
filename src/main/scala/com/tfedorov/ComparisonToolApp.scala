package com.tfedorov

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object ComparisonToolApp extends App {

  private val spark =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._

  private val originalDS = spark.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/table1.csv")

  originalDS.printSchema()
  //originalDS.show()
  //originalDS.groupBy($"zip_code").agg(max("customer_id")).show

  private val part = Window.partitionBy($"id")
  private val resultDF = originalDS.withColumn("number", count("*").over(part))
    //.filter($"number" === 2)
    .withColumn("diff", collect_set("val2").over(part))
    .withColumn("different", size($"diff"))
    .withColumn("status",
      when($"number" === 1, "no enough rows")
        .otherwise(
          when($"number" === 2 && $"different" === 1, "equal")
            .otherwise(
              when($"number" === 2 && $"different" === 2, "not equal")
                .otherwise("extra rows"))
        )
    )

  resultDF.show()
  //resultDF.explain(true)

  originalDS.createTempView("table1")
  val compareSql =
    """SELECT
      |  *,
      |  count(*) OVER (PARTITION BY id) as number,
      |  collect_set(val2) OVER (PARTITION BY id) as diff,
      |  size(collect_set(val2) OVER (PARTITION BY id)) as different
      |FROM
      |  table1""".stripMargin
  val compareDF = spark.sql(compareSql)
  compareDF.createTempView("compare")

  val statusSql =
    """SELECT
      |  *,
      |  CASE WHEN number = 1 THEN 'no enough rows' WHEN number = 2 AND different =1 THEN 'equal' WHEN number = 2 AND different =2 THEN 'not equal' ELSE 'extra rows' END AS status
      |FROM
      |  compare""".stripMargin

  spark.sql(statusSql).show

}
