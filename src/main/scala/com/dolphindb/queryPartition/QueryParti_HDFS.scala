package com.dolphindb.queryPartition

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object QueryParti_HDFS {


  def main(args: Array[String]): Unit = {

    val beginTime = System.currentTimeMillis()
    println("起始时间   ====>   " + beginTime)

    val spark = SparkSession.builder().appName("QueryParti_HDFS")
      .getOrCreate()

    import  spark.implicits._


    val origFrame = spark.read
      .parquet(s"hdfs://cnserver2:9000/chao/dolphindb/TAQ/TAQ2007${args(0)}")



//    origFrame.createOrReplaceTempView("TAQ")
    origFrame.printSchema()

    println("=====PARTITION=====")
    origFrame.foreachPartition(x =>  println("partititonID : " + TaskContext.get().partitionId()))

    println("=====ALL PARTITION=====" +origFrame.rdd.partitions.length)

    origFrame.show()

    val endTime  = System.currentTimeMillis()
    println("结束时间   ====>   " +endTime)
    println("所有时间   ====>   " + (endTime-beginTime))


    spark.stop()


  }
}
