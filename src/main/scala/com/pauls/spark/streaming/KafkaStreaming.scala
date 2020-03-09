package com.pauls.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaStreaming {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Kafka Stream").setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "ubuntu-server-003:2181",
      "group_001",
      Map("kafka-ml-test"->2),
      StorageLevel.MEMORY_AND_DISK
    )

    val words = kafkaStream.flatMap(t => t._2.split(" "))

    val counts = words.map(t => (t, 1))

    val sums = counts.reduceByKey(_ + _)

    sums.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
