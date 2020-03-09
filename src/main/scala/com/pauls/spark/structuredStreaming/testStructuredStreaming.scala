package com.pauls.spark.structuredStreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.streaming.Trigger

object testStructuredStreaming {

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder
      .master("local[*]").appName("KafkaStream").getOrCreate()

    import spark.implicits._

    val dfinput: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val wordCount = dfinput.as[String].flatMap(_.split(" ")).groupBy("value").count()

    val output = wordCount.writeStream
      .format("console")
      // update: only aggregate the latest value
      // complete: aggregate the whole value
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start

    output.awaitTermination()
  }
}
