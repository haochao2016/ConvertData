package test.connector

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import parquet.io.api.Binary

object testDateType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testDateType")
      .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

import spark.implicits._

    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("birth", DateType, nullable = true),
        StructField("time", TimestampType, nullable = true)
      ))
    println("  StructType length :  " + schema.length)

    for(i <- schema) {
      println(i)
    }



    val data = Seq(
      Row(1, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-09-30 03:03:00.230")),
      Row(1, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-12-14 03:03:00.230")),
      Row(2, Date.valueOf("2016-12-14"), Timestamp.valueOf("2016-12-14 03:03:00.231")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    //df.select("birth")//.asInstanceOf[TimestampType].asInstanceOf[LongType]
    val df1 = df.sort($"birth".desc)
    df1.show()

    df.repartition()


  }

}
