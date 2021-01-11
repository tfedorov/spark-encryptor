package com.tfedorov

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

object ShowTablesAppTest extends App {

  @Test
  def tableCounts(): Unit = {

    ShowTablesApp.main(Array.empty)

    val session = SparkSession.builder.getOrCreate()
    val actualResult = session.sql("SHOW TABLES").collect()
    assertEquals(1, actualResult.length)
  }
}
