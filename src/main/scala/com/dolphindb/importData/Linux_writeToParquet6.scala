package com.dolphindb.importData

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Linux_writeToParquet6 {


  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("Linux_writeToParquet6")
      //      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val hdds = Array("0813","0814","0815", "0816")

    for (hdd <- hdds) {
      val dataDF = spark.read
        .option("header", "true")
        .csv(s"file:///hdd/hdd5/Spark-DolphinDB-TAQ/TAQ2007${hdd}.csv")

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
      println("==========SCHEMA==============")
      dataDF1.printSchema()
      println("==========SCHEMA==============")


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

      println("==========SCHEMA==============")
      dataDF2.printSchema()
      println("==========SCHEMA==============")
      dataDF2.write.parquet(s"hdfs://cnserver2:9000/chao/dolphindb/TAQ/TAQ2007${hdd}")
    }

    val endTime = System.currentTimeMillis()
    println("===ALL import Time===  " + (endTime-startTime))
    val writer = new PrintWriter(new File("/home/chao/apps/spark/writer6.txt"))
    val allTime = endTime-startTime

    writer.write( s"""
                     |===Start Time ==== : ${startTime}
                     |===End Time ==== : ${endTime}
                     |===ALL -import- Time=== : ${allTime}
                  """.stripMargin)
    writer.close()
    spark.stop()

  }

}

