package com.dolphindb.hive.convert

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object ConvertDataToHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ConvertToHive")
//      .master()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://chao/hive/warehouse")
      .getOrCreate();

    import  spark.implicits._;

    val arr =  Array("08","09","10","13","14","15","16","17","20","21","22","23","24","27","28","29","30")

    //    sparkcsv.write.mode(SaveMode.Append).format("parquet").partitionBy("date", "symbol").insertInto("taq")

    for (arri <- arr) {
//      val sparkcsv = spark.read.option("header", "true").csv(s"file:///home/chao/data/TAQ200708${arri}.csv.gz")
      val sparkcsv = spark.read.parquet(s"hdfs://cnserver2:9000/chao/compData/TAQ200708${arri}")
      sparkcsv.createOrReplaceTempView(s"sparkCSV${arri}")
      spark.sql(s"insert into taq partition (date='200708${arri}', symbol) select time,bid,ofr, bidsiz , mode, ex, mmid, ofrsiz , symbol from sparkCSV${arri}")
      spark.catalog.dropTempView(s"sparkCSV${arri}")

    }

//  spark.stop()

  }

}
