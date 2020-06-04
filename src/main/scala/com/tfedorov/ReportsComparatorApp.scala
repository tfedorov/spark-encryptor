package main.scala.com.tfedorov

import main.scala.com.tfedorov.reports.KeyColumns
import main.scala.com.tfedorov.reports.Splitter._
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ReportsComparatorApp extends App with Logging {

  log.warn("*******Start : " + this.getClass.getCanonicalName + "*******")

  private val session = SparkSession.builder.master("local")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  private val THOMAS_INPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/PH220118.NATL.TXT"
  private val EMR_INPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/emr_file"
  private val THOMAS_OUTPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/checked/thomas_output"
  private val EMR_OUTPUT_PATH = "/Users/tfedorov/IdeaProjects/docs/test_data/checked/emr_output"


  val thomasRDD = session.sparkContext.textFile(THOMAS_INPUT_PATH)
  val emrRDD = session.sparkContext.textFile(EMR_INPUT_PATH).distinct()

  val thomSplittedRDD = thomasRDD.map(extractThomas).map { row => (row.keyColumns, row) }
  val emrSplittedRDD = emrRDD.map(extractEmr).map { row => (row.keyColumns, row) }

  private val union = thomSplittedRDD.union(emrSplittedRDD)
  val groupedRDD = union.groupByKey()

  val thomasUniqueRDD = groupedRDD.filter(!_._2.exists(_.isFromEmr))
  val emrUniqueRDD = groupedRDD.filter(!_._2.exists(_.isFromThomas))

  implicit val ordering: Ordering[KeyColumns] = new Ordering[KeyColumns] {
    override def compare(x: KeyColumns, y: KeyColumns): Int = {
      val compareByDate = x.commercialDate.compareTo(y.commercialDate)
      if (compareByDate == 0)
        x.commercialTime.compareTo(y.commercialTime)
      else
        compareByDate
    }
  }

  private val petitioner = new HashPartitioner(1)
  thomasUniqueRDD.repartitionAndSortWithinPartitions(petitioner)
    .flatMapValues(_.map(_.originalRow)).repartition(1).saveAsTextFile(THOMAS_OUTPUT_PATH)
  emrUniqueRDD.repartitionAndSortWithinPartitions(petitioner)
    .flatMapValues(_.map(_.originalRow)).repartition(1).saveAsTextFile(EMR_OUTPUT_PATH)

  /*
    println("All together: " + union.count())
    groupedRDD.cache()
    println("Grouped all  : " + groupedRDD.count())
    println("Grouped emr & thomas 2 : " + groupedRDD.filter { kv => kv._2.size == 2 && kv._2.exists(_.isFromEmr) && kv._2.exists(_.isFromThomas) }.count())
    println("Grouped emr & thomas 3+ : " + groupedRDD.filter { kv => kv._2.size > 2 && kv._2.exists(_.isFromEmr) && kv._2.exists(_.isFromThomas) }.count())
    println("only 1: " + groupedRDD.filter { kv => kv._2.size == 1 }.count())
    println("only 1 thomas: " + groupedRDD.filter { kv => kv._2.size == 1 && kv._2.exists(_.isFromThomas) }.count())
    println("only 1 emr : " + groupedRDD.filter { kv => kv._2.size == 1 && kv._2.exists(_.isFromEmr) }.count())

    //println("strange: " + groupedRDD.filter { kv => kv._2.filter(_.isFromThomas).size >1}.count())
    println("ThomasUnique: " + thomasUniqueRDD.count())
    println("EmrUnique: " + emrUniqueRDD.count())
  */
}
