package test.connector

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object TestSparkDateType {


  def main(args: Array[String]): Unit = {

    println( "   date     " + Date.valueOf("2007-08-01"))


    val spark = SparkSession.builder().appName("").master("local[1]").getOrCreate()

    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("birth", DateType, nullable = true),
        StructField("time", TimestampType, nullable = true)
      ))

    val data = Seq(
      Row(1, Date.valueOf("2012-12-12"), Timestamp.valueOf("2016-09-30 03:03:00")),
      Row(2, Date.valueOf("2016-12-14"), Timestamp.valueOf("2016-12-14 03:03:00")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    df.printSchema()
    df.show()
    df.createTempView("df")

//    val time = Date.valueOf("20121212")
//    val frame = spark.sql(s"select * from df where time='2016-12-14 03:03:00'")
    val frame = spark.sql(s"select * from df where birth>'2016-12-13'")
    frame.show()







  }

}
