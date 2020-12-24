package com.tfedorov

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.StdIn

object ReadCSVApp extends App {

  private val session: SparkSession = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  import session.sqlContext.implicits._

  session.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)
  session.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
  session.conf.set("spark.sql.shuffle.partitions", "3")

  private val table1DF = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/table1.csv")
    .repartition(3, partitionExprs = 'id)

  //  table1DF.printSchema()
  //println(table1DF.rdd.getNumPartitions)
  // table1DF.createTempView("table1")
  // table1DF.write.option("header", "true").csv("/Users/tfedorov/IdeaProjects/tmp/t1")

  private val table2DF = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/table2.csv")
    .repartition(3, partitionExprs = 'id)
  //  table2DF.printSchema()
  //  table2DF.createTempView("table2")
  //println(table2DF.rdd.getNumPartitions)
  //  table2DF.write.option("header", "true").csv("/Users/tfedorov/IdeaProjects/tmp/t2")
  private val joinedDF = table1DF.join(table2DF, table1DF("table2_id") === table2DF("id"))
  //joinedDF.show
  private val resultDF = joinedDF.groupBy("cat_value").agg(sum("val2"))
  //resultDF.explain()
  resultDF.show


  StdIn.readLine()
}
