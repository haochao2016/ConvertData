package test

import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

object TestSparkSQLFun {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestSparkSQLFun")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val frame = spark.sql("select to_timestamp('2019-04-12 09:30:45')")


    frame.show()



  }

}
