package com.dolphindb.convert

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object WIN_Avg_OFR_ByOFRSIZ {

  def main(args: Array[String]): Unit = {
    println("==========================" +System.currentTimeMillis())
    val spark = SparkSession.builder().appName("WIN_TableTempTAQ")
      .master("local[3]")
      .getOrCreate()

    val parquetframe = spark.read
//      .option("header", "true")
      .parquet("D:\\data\\t.csv\\20190301-A-parquet")

    parquetframe.printSchema()
//    parquetframe.show(1000)

    println("=========================================")

    parquetframe.createOrReplaceTempView("parquetTab")

//    spark.sql("select avg(OFR),OFRSIZ from parquetTab group by OFRSIZ").show()
    spark.sql("select date,time, OFR ,OFRSIZ from parquetTab where time > '8:00:12'").show()


//    parquetframe.write.format("csv").save("D:\\data\\t.csv\\1.csv")

//    println("============partition===========")
//    parquetframe.foreachPartition( x => println(  TaskContext.getPartitionId()))

    spark.stop()
  }

}
