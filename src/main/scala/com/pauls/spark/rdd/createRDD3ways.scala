package com.pauls.spark.rdd

import com.pauls.spark.rdd.createRDD3ways.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object createRDD3ways extends App {
  // first way: from the memory
  val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
    .set("spark.default.parallelism", "20")
  val sc = new SparkContext(conf)
//  createRDDFromMemory(sc)
  createRDDFromExternalFile(sc)

  def createRDDFromMemory(sc:SparkContext): Unit ={
    // parallelize
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,65))
    rdd1.foreach(println)
    // makdRDD same as parallelize
    val rdd2 = sc.makeRDD(Array(1,2,3,4,5,65))
    rdd2.saveAsTextFile("outout333")
  }

  def createRDDFromExternalFile(sc: SparkContext):Unit = {
    // could be reading the txt file from the resource dir,
    // for hdfs files, must specify the whole url like hdfs://hadoop-master:9000/release/txt.txt
    val rdd3 = sc.textFile("hdfs://hadoop-master:9000/user/pauls/test/hive",2)
    rdd3.saveAsTextFile("output")
  }


}
