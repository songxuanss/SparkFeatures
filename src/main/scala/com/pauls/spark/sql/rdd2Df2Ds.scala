package com.pauls.spark.sql

import com.pauls.sparkBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object rdd2Df2Ds extends App with sparkBase{

  testDF2DS

  def testRDD2DS2DF() {
    val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
      .set("spark.default.parallelism", "2")
    sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawRdd = sc.makeRDD(List((1, "pauls", 34),(2, "haisheng", 41),(1, "john", 38)))
    val structedRdd: RDD[People] = rawRdd.map(t => {People(t._1, t._2,t._3)})

    val df = structedRdd.toDF()
    val ds = structedRdd.toDS()

    df.show()
    ds.show()

    val alteredDfRDD = df.rdd
    val alteredDsRDD = ds.rdd

    alteredDfRDD.foreach(
      row => {
        println(row.getString(1))
      }
    )

    // for Ds, it is case class instead of row, so we can get the value according to the variables
    alteredDsRDD.foreach(
      people => {
        println(people.id)
      }
    )
  }

  def testDF2DS: Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
      .set("spark.default.parallelism", "2")
    sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawRdd = sc.makeRDD(List((1, "pauls", 34),(2, "haisheng", 41),(1, "john", 38)))
    val structedRdd: RDD[People] = rawRdd.map(t => {People(t._1, t._2,t._3)})
    val df = structedRdd.toDF()

    val ds = df.as[People]
    ds.show()
  }

  case class People(id:Int, name:String, age:Int)
}

