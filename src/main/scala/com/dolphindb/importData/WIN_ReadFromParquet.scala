package com.dolphindb.importData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object WIN_ReadFromParquet {
  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("WIN_ReadFromParquet")
      .master("local[2]")
      .getOrCreate()

    /*val dataSchema = StructType(
      StructField("symbol", StringType)
        :: StructField("date", DateType)
        :: StructField("time", TimestampType)
        :: StructField("bid", DoubleType)
        :: StructField("ofr", DoubleType)
        :: StructField("bidsiz", IntegerType)
        :: StructField("ofrsiz", IntegerType)
        :: StructField("mode", IntegerType)
        :: StructField("ex", ByteType)
        :: StructField("mmid", StringType)
        :: Nil)*/

    val dataDF = spark.read
//      .schema(dataSchema)
//      .option("header", "true")
//      .csv("D:\\data\\t.csv\\spark-origin-data.csv")
        .parquet("D:\\data\\t.csv\\20190412\\parq1")


    println("==========SCHEMA==============")
    dataDF.printSchema()
    println("==========SCHEMA==============")
    dataDF.show()





  }

}
