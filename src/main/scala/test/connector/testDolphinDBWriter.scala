package test.connector

import org.apache.spark.sql.SparkSession

object testDolphinDBWriter {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DolphinDBWriter")
      .master("local[2]")
      .getOrCreate()

    val originDF = spark.read
      .option("header", "true").csv("D:\\data\\t.csv\\spark-origin-data1.csv")

    originDF.printSchema()
    originDF.show()

    originDF.write
      .format("com.dolphindb.spark.DolphinDBDataSourceV2")

      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("passwd", "123456")
      .option("tablePath", "dfs://chao/Spark/db/sparktb1")
//      .option("dbPath" , "dfs://chao/Spark/db")
//      .option("table", "sparktb1")
      .save()



  }
}
