package com.tfedorov.streaming

import com.tfedorov.blotto.BlottoGameApp.log
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

object StreamingApp extends App  with Logging {

  log.warn("*******Start : " + this.getClass.getSimpleName + "*******")

  private val session = SparkSession.builder.master("local[*]")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  import session.sqlContext.implicits._

  val streamingContext = new StreamingContext(session.sparkContext, Seconds(1))

  PaymentDeserializer.init(schemaRegUrl = "http://localhost:8081")

  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[PaymentDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val topics = Array("my-topic")


  private val stream = KafkaUtils.createDirectStream[String, Payment](
    streamingContext,
    PreferConsistent,
    Subscribe[String, Payment](topics, kafkaParams)
  )

  stream.foreachRDD { rdd =>
    if (rdd.count() > 0) {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
      val min = offsetRanges.map(_.fromOffset).min
      val max = offsetRanges.map(_.untilOffset).max
      rdd.map(_.value()).toDF().as[Payment]
        .write.json("file:///Users/tfedorov/IdeaProjects/Hdfs3SinkConnector/" + min + "-" + max)
    }
  }
  session.sparkContext.getConf.getAll.foreach(println)
  streamingContext.start()
  streamingContext.awaitTermination()

  log.warn("*******End*******")
}
