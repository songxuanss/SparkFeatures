package com.pauls.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socketstreaming {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming Socket case")

    // Seconds(3) defines the interval to fetch the data
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    val words = socketDStream.flatMap(_.split(" "))

    val maps = words.map((_,1))

    val sums = maps.reduceByKey(_ + _)

    sums.print()

    streamingContext.start()
    streamingContext.awaitTermination()

    // use nc -lk 9999 to write something into the socket port 9999

  }
}
