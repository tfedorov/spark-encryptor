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

  val originalDS = spark.read.option("header", "true").option("inferschema", "true")
    .csv("/Users/tfedorov/IdeaProjects/spark-encryptor/src/main/resources/duke/customers.txt")

  originalDS.printSchema()
  //originalDS.show()
  //originalDS.groupBy($"zip_code").agg(max("customer_id")).show

  val part = Window.partitionBy($"id")
  val result = originalDS.withColumn("number", count("*").over(part))
    //.filter($"number" === 2)
    .withColumn("diff", collect_set("val1").over(part))
    .withColumn("different", size($"diff"))
    .withColumn("status",
      when($"number" === 1, "no enough rows")
        .otherwise(
          when($"number" === 2 && $"different" === 1, "equal")
            .otherwise(
              when($"number" === 2 && $"different" === 2, "not equal").otherwise("extra rows"))
        )
    )

  result.show()
  result.explain(true)

  originalDS.createTempView("union")
  spark.sql("S")
}
