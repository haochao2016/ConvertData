package test.spark

import org.apache.spark.sql.SparkSession

object TestSparkSQLFUN {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestSparkSQLFUN")
      .master("local[3]")
      .getOrCreate()

//    val frame = spark.sql("select to_timestamp('00:12:00') ")
//    val frame = spark.sql("select CAST('00:12:00' AS TIMESTAMP)")
//    val frame = spark.sql("select minute('00:12:00')")
//    val frame = spark.sql("SELECT date_trunc('SECOND', '2015-03-05T09:32:05.359')")
//    val frame = spark.sql("SELECT to_date('2009-07-30 04:17:52', 'HH:mm:ss')")
    val frame = spark.sql("SELECT to_timestamp('2009-07-30 04:17:52' )")
    frame.show()
  }
}
