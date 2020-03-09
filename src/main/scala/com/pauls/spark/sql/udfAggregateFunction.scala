package com.pauls.spark.sql

import com.pauls.spark.sql.rdd2Df2Ds.{People, sc}
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object udfAggregateFunction extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("create RDD")
    .set("spark.default.parallelism", "2")
  sc = new SparkContext(conf)
  val spark = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  val rawRdd = sc.makeRDD(List((1, "pauls", 34),(2, "haisheng", 41),(1, "john", 38)))
  val structedRdd: RDD[People] = rawRdd.map(t => {People(t._1, t._2,t._3)})

  val udfAveAge = new MyAveAgeFunction()
  spark.udf.register("aveAge", udfAveAge)

  val df = structedRdd.toDF()

  df.createTempView("users")

  // after registered, we can use the udf in the spark sql
  spark.sql("select aveAge(age) from users").show()
}

case class People(id:Int, name:String, age:Int)

class MyAveAgeFunction extends UserDefinedAggregateFunction {

  // the parameter that used as input
  override def inputSchema: StructType = {
    new StructType().add("age", "Int")
  }

  // the parameter that used as median value
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", "Int")
  }

  // the function return type
  override def dataType: DataType = {
    DoubleType
  }

  // does the function stable or not?
  override def deterministic: Boolean = true

  // the buffer initialzation
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // the order of first buffer is sum
    buffer(0) = 0L
    // the order of the second buffer is count
    buffer(1) = 0

  }

  // update the buffer according to content in each row
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // sum will be the value added
    buffer(0) = buffer.getLong(0) + input.getInt(0).toLong
    // count will be just count
    buffer(1) = buffer.getInt(1) + 1
  }

  // merge buffer from multiple workers
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // calculation function
  override def evaluate(buffer: Row): Any = {
    (buffer.getLong(0) / buffer.getInt(1).toLong).toDouble
  }
}
