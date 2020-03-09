package com.pauls.spark.sql

import com.pauls.spark.sql.rdd2Df2Ds.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object udfAggregateWithTypeFunction {
  case class People(id:Int, name:String, age:Int)

  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
      .set("spark.default.parallelism", "2")
    sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawRdd = sc.makeRDD(List((1, "pauls", 34),(2, "haisheng", 41),(1, "john", 38)))
    // TODO: NOTE should use UserBean as the class of the RDD
    val structedRdd: RDD[UserBean] = rawRdd.map(t => {UserBean(t._1, t._2,t._3)})
    // TODO: NOTE type agg udf should use ds with dsl queries
    val ds = structedRdd.toDS()
    val udfAveAge = new AveAgeTypeFunction()

    // TODO: NOTE Change the udfAveAge function to column
    val aveCol:TypedColumn[UserBean, Double] = udfAveAge.toColumn.name("ave")

    // TODO: NOTE USE DSL type to select the data
    ds.select(aveCol).show()
  }
}

case class UserBean(id:Int, name:String, age: Int)
case class BufferBean(var sum: Long, var count: Int)

 class AveAgeTypeFunction extends Aggregator[UserBean, BufferBean, Double]{

   override def zero: BufferBean = {BufferBean(0L, 0)}

   override def reduce(b: BufferBean, a: UserBean): BufferBean = {
     b.sum = b.sum + a.age
     b.count = b.count + 1

     b
   }

   override def merge(b1: BufferBean, b2: BufferBean): BufferBean = {
     b1.sum = b1.sum + b2.sum
     b1.count = b1.count + b2.count

     b1
   }

   override def finish(reduction: BufferBean): Double = {
     reduction.sum.toDouble / reduction.count
   }

   override def bufferEncoder: Encoder[BufferBean] = Encoders.product

   override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
 }