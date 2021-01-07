package com.tfedorov.tutorial

import com.tfedorov.utils.FilesUtils
import org.apache.spark.sql.SparkSession

//https://www.w3resource.com/sql-exercises/subqueries/index.php#SQLEDITOR
object SubQueryApp extends App {

  private val localEnv = sys.env.getOrElse("SPARK_LOCAL", "false")
  println("SPARK_LOCAL=" + localEnv)
  println("*********")

  println(FilesUtils.readAssemblyManifest())
  println("*********")

  private val session: SparkSession =
    SparkSession.builder.master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

  import org.apache.spark.sql.functions._
  import session.sqlContext.implicits._

  private val detailDS = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/subQ/emp_details.csv").withColumn("all", lit("all"))
  detailDS.createTempView("emp_details")

  private val departDF = session.read.option("header", "true").option("inferschema", "true")
    .csv("src/main/resources/subQ/emp_department.csv")
  departDF.createTempView("emp_department")

  val subQuerySQL =
    """SELECT * FROM emp_details
      |WHERE
      |  EMP_DEPT = (
      |    SELECT
      |      DPT_CODE
      |    FROM
      |      emp_department
      |    WHERE
      |      DPT_ALLOTMENT = (
      |        SELECT
      |          MIN(dpt_allotment)
      |        FROM
      |          emp_department
      |        WHERE
      |          dpt_allotment > (
      |            SELECT
      |              MIN(dpt_allotment)
      |            FROM
      |              emp_department
      |          )
      |      )
      |  )""".stripMargin
  val subDF = session.sql(subQuerySQL)
  subDF.show

  val windowFunctionSQL =
    """SELECT
      |  dt.EMP_FNAME,
      |  dt.EMP_LNAME,
      |  DENSE_RANK() OVER (
      |    PARTITION BY dt.all
      |    ORDER BY
      |      DPT_ALLOTMENT
      |  ) AS DENSE_RANK
      |FROM
      |  emp_details dt
      |  JOIN emp_department dp ON dt.EMP_DEPT = dp.DPT_CODE""".stripMargin
  val partitionDF = session.sql(windowFunctionSQL).where($"DENSE_RANK" === 2)
  partitionDF.show


}
