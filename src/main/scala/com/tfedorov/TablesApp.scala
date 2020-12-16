package com.tfedorov

import org.apache.spark.sql.SparkSession

object TablesApp extends App {


  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()


  val originalDS = session.read.option("header", "true").option("inferschema", "true")
    .csv("/Users/tfedorov/IdeaProjects/spark-encryptor/src/main/resources/compar/customers.txt")
  originalDS.createTempView("original")

  session.range(10).createTempView("changes")
  session.sql("SHOW TABLES").show


  case class Table(database: String, tableName: String, isTemporary: String)

  import session.sqlContext.implicits._

  val tables = session.sql("SHOW TABLES").as[Table].collect().map(_.tableName)

  tables.foreach { table => session.sql(s"SHOW COLUMNS IN $table").withColumnRenamed("col_name", table).show }
  tables.foreach { table => session.sql(s"DESCRIBE TABLE EXTENDED $table").show }
}
