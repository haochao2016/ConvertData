package test.spark

import org.apache.spark.sql.SparkSession

object TestSparkSQLFUN {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestSparkSQLFUN")
      .master("local[3]")
      .getOrCreate()

//    val frame = spark.sql("select to_timestamp('00:12:00') ")
//    val frame = spark.sql("select CAST('00:12:00' AS TIMESTAMP)")
    val frame = spark.sql("select minute('00:12:00')")
    frame.show()
  }
}
