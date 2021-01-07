package com.tfedorov.tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object Read2FilesCSVApp extends App {

  private val session: SparkSession = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  import session.sqlContext.implicits._

  session.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)
  session.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
  session.conf.set("spark.sql.shuffle.partitions", "3")

  private val table1DF = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/table1.csv")
    .repartition(2, partitionExprs = 'id)


  private val table2DF = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/table2.csv")
    .repartition(3, partitionExprs = 'id)

  private val joinedDF = table1DF.join(table2DF, table1DF("table2_id") === table2DF("id"))
  joinedDF.explain()

  private val resultDF = joinedDF.groupBy("cat_value").agg(sum("val2"))

  resultDF.show

}
