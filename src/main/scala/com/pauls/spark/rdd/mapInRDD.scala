package com.pauls.spark.rdd

import com.pauls.sparkBase
import org.apache.spark.{SparkConf, SparkContext}

object mapInRDD extends App with sparkBase{

  testMapFunction(sc)

  def testMapFunction(sc:SparkContext): Unit ={
    val initRdd = sc.makeRDD(Array(1,2,3,4,5,6))

    val newRdd = initRdd.map(_*2)

    newRdd.foreach(println)
  }
}
