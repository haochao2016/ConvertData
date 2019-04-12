package com.dolphindb.importData

import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object WIN_writeToParquet {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("WIN_writeToParquet")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val dataSchema = StructType(
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
                                :: Nil)

    val dataDF = spark.read
      .option("header", "true")
      .csv("D:\\data\\t.csv\\spark-origin-data.csv")

    val dataDF1 = dataDF.map(it => {
      (it.getString(0),
      it.getString(1).substring(0,4) + "-" + it.getString(1).substring(4,6)+ "-" + it.getString(1).substring(6),
        it.getString(2),
        it.getString(3),
        it.getString(4),
        it.getString(5),
        it.getString(6),
        it.getString(7),
        it.getString(8).charAt(0).toByte,
        it.getString(9)
      )
    }).toDF("symbol","date", "time", "bid", "ofr", "bidsiz", "ofrsiz", "mode", "ex", "mmid")



    println("==========SCHEMA==============")
    dataDF.printSchema()
    dataDF.show()
    println("==========SCHEMA==============")
    dataDF1.printSchema()
    dataDF1.show()



    val dataDF2 = dataDF1.select(
      dataDF1.col("symbol").cast(StringType),

      dataDF1.col("date").cast(DateType),
      dataDF1.col("time").cast(TimestampType),

      dataDF1.col("bid").cast(DoubleType),
      dataDF1.col("ofr").cast(DoubleType),
      dataDF1.col("bidsiz").cast(IntegerType),
      dataDF1.col("ofrsiz").cast(IntegerType),
      dataDF1.col("mode").cast(IntegerType),
      dataDF1.col("ex").cast(ByteType),
      dataDF1.col("mmid").cast(StringType)
    )

//    dataDF.select("date").



    println("==========SCHEMA==============")
//    val data = dataDF2.as[TAQ]
    dataDF2.printSchema()
    dataDF2.show()
    println("==========SCHEMA==============")
    dataDF2.write.parquet("D:\\data\\t.csv\\20190412\\parq1")

  }


}

case class TAQ (symbol : String, date : Date,
                time : Timestamp, bid : Double,
                ofr :Double, bidsiz : Int,
                ofrsiz : Int, mode : Int,
                ex : Byte, mmid : String
               )