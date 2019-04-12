package test.connector

import org.apache.spark.sql.SparkSession

object testSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("csvFile")
      .master("local[3]")
      .getOrCreate()


//    val parquetframe = spark.read.parquet("D:\\data\\t.csv\\20190301-A-parquet")
    val parquetframe = spark.read.format("parquet").load("D:\\data\\t.csv\\20190301-A-parquet")

//    spark.read.jdbc()


    parquetframe.show()

    spark.stop()


  }

}
