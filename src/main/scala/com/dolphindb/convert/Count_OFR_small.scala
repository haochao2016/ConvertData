package com.dolphindb.convert

import org.apache.spark.sql.SparkSession

object Count_OFR_small {

  def main(args: Array[String]): Unit = {

    println("起始时间====>   " +  System.currentTimeMillis() )

    val spark = SparkSession.builder().appName("Count_OFR_small")
      .getOrCreate()

   val frame27 = spark.read.parquet("hdfs://myubuntu:9000/chao/testData/orig/TAQ20070827")
//    frame27.printSchema()
    println("所有的条数  ： " + frame27.count())

//    frame27.createOrReplaceTempView("TAQ27")
  /*  val frame27Num = frame27.filter( func = row =>
      try {
        row.getString(6).toInt > 1
      } catch {
        case ex : NumberFormatException => println("not a numble")
        case ex : AnyVal => print(234)
      }*/


//     ).count()

//    frame27.rdd.zipWithIndex()
    val defaultParallelism = spark.sparkContext.defaultParallelism

    val arr = frame27.take(defaultParallelism)
    val frame27Num = frame27.filter(row => {! row.getString(6).equals("OFRSIZ")}).filter(row => row.getString(6).toInt > 1).count()




    println("OFRSIZ 中大于0 的条数  ：  " + frame27Num )

    println("结束时间====>   " +  System.currentTimeMillis() )

    spark.stop()
  }

}
