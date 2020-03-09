package com.pauls.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{
  val conf:SparkConf  = new SparkConf()
//    .setMaster("spark://192.168.31.114:7077")
    .setAppName("work-count-1")
//    conf.set("spark.driver.host", "192.168.31.114")
    conf.setMaster("local[*]")
  val sc:SparkContext = new SparkContext(conf)

  val text:RDD[String] = sc.textFile("/home/pauls/work/words")
  /**
   * map and flatmap
   * map : deal rdd line by line, so the result will be array[array]
   * flatmap: deal rdd regardless of lines, so the result will be array[]
   */
//  val words2: Array[Array[String]] = text.map(_.split(" ")).collect()
  val words: RDD[String] = text.flatMap(_.split(" "))

  val words2One: RDD[(String, Int)] = words.map((_,1))

  val words2sum = words2One.reduceByKey(_+_)

  words2sum.foreach(println)
}
