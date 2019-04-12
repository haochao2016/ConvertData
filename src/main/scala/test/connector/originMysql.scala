package test.connector

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object originMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("originMysql")
      .master("local[2]")
      .getOrCreate()

    val url = "jdbc:mysql://115.239.209.189:3306/spark-conn?user=root&password=123456"
    val frame = spark.read
//        .option("partitionColumn", "code")
//      .option("lowerBound", 0)
//      .option("upperBound", 6)
//      .option("numPartitions", 3)
//      .jdbc(url, "mysqlConn", new Properties())
      .jdbc(url, "person", new Properties())
//    spark.read.jdbc
    frame.printSchema()

//    frame.select("id").show()


//    frame.foreachPartition(part => {
//      println( "分区数  "  + TaskContext.get().partitionId())
//      part.foreach(p => println(p))
//    })
//    frame.show()


    frame.createOrReplaceTempView("frame")
    //    spark.sql("select * from frame where date1 <'2019-04-03'").show()
    val mysqlframe = spark.sql("select * from frame where id <'5'")
    mysqlframe.show()
    mysqlframe.write.option("header", "true").mode("overwrite")
      .format("csv").save("D:\\data\\t.csv\\11")




  }

}
