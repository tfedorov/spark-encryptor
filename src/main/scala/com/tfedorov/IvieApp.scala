package com.tfedorov

import com.tfedorov.ComparisonToolApp.part
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}

object IvieApp extends App {

  import org.apache.spark.sql.SparkSession

  private val spark = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  case class Module(unit_series: String, module_name: String, module_number: String, module_timestamp: String)

  val modules = Seq(
    Module("ABC", "JustName1", "DFE", "2020 01 05"),
    Module("DFE", "JustName2", "QWE", "2020 01 02"),
    Module("DFE", "JustName3", "ASD", "2020 01 03"),
    Module("QAZ", "JustName4", "GFD", "2020 01 04")
  )

  case class UnitTests(unit_series: String, station_name: String, test_timestamp: String, test_result: Boolean)

  val tests = Seq(
    UnitTests("ASD", " Station1", " 2020 01 07", true),
    UnitTests("ABC", " Station1", " 2020 01 01", true),
    UnitTests("DFE", " Station1", " 2020 01 02", true),
    UnitTests("ASD", " Station1", " 2020 01 03", true),
    UnitTests("DFE", " Station2", " 2020 01 04", true),
    UnitTests("ASD", " Station2", " 2020 01 05", true)
  )

  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val modulesDF = spark.createDataFrame(modules).toDF()
  val testsDF = spark.createDataFrame(tests).toDF()
  modulesDF.as("m1").join(modulesDF.as("m2"), $"m1.module_number" === $"m2.unit_series").show

  val partition = Window.partitionBy("m2.module_number", "t1.station_name").orderBy("t1.test_timestamp")
  modulesDF.as("m1")
    .join(modulesDF.as("m2"), $"m1.module_number" === $"m2.unit_series")
    .join(testsDF.as("t1"), $"m2.module_number" === $"t1.unit_series")
    .withColumn("part", row_number().over(partition))
    .show

  val partitionTest = Window.partitionBy("m1.module_number").orderBy(to_date($"t1.test_timestamp", "yyyy MM dd").asc)
  modulesDF.as("m1")
    .join(testsDF.as("t1"), $"m1.module_number" === $"t1.unit_series")
    .where(to_date($"m1.module_timestamp", "yyyy MM dd") < to_date($"t1.test_timestamp", "yyyy MM dd"))
    .withColumn("part", row_number().over(partitionTest)).where($"part" === 1)
    //.withColumn("_part", min(to_date($"t1.test_timestamp", "yyyy MM dd")).over(partitionTest))
    //.where(to_date($"t1.test_timestamp", "yyyy MM dd") === $"_part")
    .show

}
