package com.dolphindb.convert

import org.apache.spark.sql.{DataFrame, SparkSession}

object ConvertDataToParquet {

  def main(args: Array[String]): Unit = {
    println("起始时间====>   " +  System.currentTimeMillis() )
    val spark = SparkSession.builder().appName("ConvertDataToParquet")
                  .master("local[4]")
                  .getOrCreate()

    import spark.implicits._

//  val arr =  Array("08","09","10","13","14","15","16","17","20","21","22","23","24","27","28","29","30")

    val arr =  Array("01", "02", "03", "06", "07", "31")

  for (arri <-  arr) {

    val csvFrame: DataFrame = spark.read.option("header", "true").csv(
      //        "/home/chao/20190301-data.csv"
      //      "file:///home/chao/TAQ20070801.csv"
      s"file:///hdd/hdd0/data/TAQ/TAQ200708${arri}.csv"
    )

    //    csvFrame.show()
    csvFrame.write.parquet(s"hdfs://cnserver2:9000/chao/compData/TAQ200708${arri}")
    //    csvFrame.write.csv("hdfs://myubuntu:9000/chao/testData/orig/csv")

  }

//    val winframe =  spark.read.option("header", "true").csv("D:\\data\\t.csv\\TAQ20070827.csv")
//    winframe.write.parquet("D:\\data\\t.csv\\20190827-parquet")
//    ===========WIN===============
 /*  val winframe =  spark.read.option("header", "true").csv("D:\\data\\t.csv\\20190301-data.csv")
    winframe.write.parquet("D:\\data\\t.csv\\20190301-A-parquet")*/
    println("结束时间====>   " +  System.currentTimeMillis() )
    spark.stop()
  }
}
