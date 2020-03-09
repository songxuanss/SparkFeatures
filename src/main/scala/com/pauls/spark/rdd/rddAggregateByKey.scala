package com.pauls.spark.rdd

import com.pauls.sparkBase
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object rddAggregateByKey extends App with sparkBase{

  init()
  testAggregateByKey(this.sc)

  def testAggregateByKey(sc:SparkContext): Unit ={
    val input1: RDD[Array[(String, Int)]] = sc.makeRDD(Array(Array(("a",3),("b",4),("c",8)), Array(("a",3),("b",7),("c",12))))
    val input2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",3),("c",9),("a",3),("b",7),("c",12)))

    val output = input2.aggregateByKey(0)(math.max(_,_),_+_)

    output.foreach(println)
  }
}
