package com.pauls

import com.pauls.spark.rdd.mapInRDD.testMapFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}

trait sparkBase {
  var sc:SparkContext = _
  var sparkSession:SparkSession = _
  def init() {
    val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
      .set("spark.default.parallelism", "2")
    sc = new SparkContext(conf)
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
  }
}
