package com.tfedorov

import org.apache.spark.sql.{SQLContext, SparkSession}

object ReadCSVApp extends App {

  private val session: SparkSession = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  import session.sqlContext.implicits._

  private val originalDS = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/compar/customers.csv")
  originalDS.createTempView("original")

  session.range(10).createTempView("changes")
  session.sql("SHOW TABLES").show

  private case class Table(database: String, tableName: String, isTemporary: String)

  private val tables = session.sql("SHOW TABLES").as[Table].collect().map(_.tableName)

  tables.foreach { table =>
    session.sql(s"SHOW COLUMNS IN $table")
      .withColumnRenamed("col_name", table).show
  }
  tables.foreach { table => session.sql(s"DESCRIBE TABLE EXTENDED $table").show }
}
