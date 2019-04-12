package com.dolphindb.convert

import org.apache.spark.sql.SparkSession

object WIN_TableTempTAQ {

  def main(args: Array[String]): Unit = {

    println("==========================" +System.currentTimeMillis())
    val spark = SparkSession.builder().appName("WIN_TableTempTAQ")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    val csvframe = spark.read.option("header", "true")
        .csv("D:\\data\\t.csv\\20190301-data.csv")

    csvframe.createOrReplaceTempView("csvv")

    csvframe.printSchema()
    csvframe.show()
    println("********************************************************")

//    val frame = spark.sql("select * from csvv where OFRSIZ = 0")

    val frame = spark.sql("select avg(OFR),OFRSIZ from csvv group by OFRSIZ")
    frame.show()


    //    val csvs1 =  csvframe.select("OFRSIZ")
//      .filter(_.getInt(0) > 0)
//      .filter{_.toString().toInt > 0}


    println("********************************************************")

//    val csvs1 = csvframe.filter(row => row.getString(6).toInt > 0)
//      .select("")

    println("********************************************************")

//    val frame0  =  csvframe.map{row => row.getString(6).toInt > 0}

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("==========================" +System.currentTimeMillis())


  }

}
