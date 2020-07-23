package main.scala.com.tfedorov

import _root_.main.scala.com.tfedorov.reports.KeysExtractor._
import main.scala.com.tfedorov.reports.Splitter._
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ReportsComparatorApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val ORIGINAL_INPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/PH220118.NATL.TXT"
  private val CHECKED_INPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/emr_file"
  private val ORIGINAL_UNIQUES_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/checked/Parsed_thomas_output"
  private val CHECKED_UNIQUES_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/checked/Parsed_emr_output"

  println(s"Read original file from '$ORIGINAL_INPUT_PATH'")
  println(s"Read checked file from '$CHECKED_UNIQUES_PATH")
  val originalFileRDD = session.sparkContext.textFile(ORIGINAL_INPUT_PATH)
  val checkedFileRDD = session.sparkContext.textFile(CHECKED_INPUT_PATH).distinct()

  val originalSplittedRDD = originalFileRDD.map(extractOriginal).map { row => (row.keys4Cols, row) }
  val checkedSplittedRDD = checkedFileRDD.map(extractChecked).map { row => (row.keys4Cols, row) }

  val unionRDD = originalSplittedRDD.union(checkedSplittedRDD)
  val groupedRDD = unionRDD.groupByKey()
  groupedRDD.cache()

  val thomasUniqueRDD = groupedRDD.filter(!_._2.exists(_.isNewSource))
  val emrUniqueRDD = groupedRDD.filter(!_._2.exists(_.isOriginalSource))

  val partitioner = new HashPartitioner(1)

  println(s"Write original uniques to '$ORIGINAL_INPUT_PATH'")
  //Ordering in KeysExtractor
  thomasUniqueRDD.repartitionAndSortWithinPartitions(partitioner)
    .flatMapValues(_.map(_.originalRow)).repartition(1).saveAsTextFile(ORIGINAL_UNIQUES_PATH)

  //Ordering in KeysExtractor
  println(s"Write checked uniques to '$CHECKED_UNIQUES_PATH'")
  emrUniqueRDD.repartitionAndSortWithinPartitions(partitioner)
    .flatMapValues(_.map(_.originalRow)).repartition(1).saveAsTextFile(CHECKED_UNIQUES_PATH)

  println("Original rows number: " + originalFileRDD.count())
  println("Checked  rows number:" + checkedFileRDD.count())
  println("Bad grouped   number: " + groupedRDD.filter { kv => kv._2.size > 2 }.count())
  println("Original rows UNIQUE: " + thomasUniqueRDD.count())
  println("Checked  rows UNIQUE: " + emrUniqueRDD.count())

}
