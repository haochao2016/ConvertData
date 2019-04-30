package test.mysql

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object testMysql {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder().appName("originMysql")
      .master("local[2]")
      .getOrCreate()

    val url = "jdbc:mysql://115.239.209.189:3306/spark-conn?user=root&password=123456"
    val frame = spark.read
//              .option("partitionColumn", "id")
//            .option("lowerBound", 0)
//            .option("upperBound", 6)
//            .option("numPartitions", 3)
            .jdbc(url, "mysqlConn", new Properties())
//      .jdbc(url, "taq", new Properties())
    //    spark.read.jdbc
    frame.printSchema()

    val frame1 = spark.read.jdbc(url, "person", new Properties())

    //    frame.select("id").show()


    //    frame.foreachPartition(part => {
    //      println( "分区数  "  + TaskContext.get().partitionId())
    //      part.foreach(p => println(p))
    //    })
    //    frame.show()



    frame.createOrReplaceTempView("frame")
    //    spark.sql("select * from frame where date1 <'2019-04-03'").show()
//    val mysqlframe = spark.sql("select * from frame where (id > '3' or id < '5') and name < '8'")
//    val mysqlframe = spark.sql("select * from frame where name > '2'")

    frame1.createOrReplaceTempView("frame1")
    val mysqlframe = spark.sql("SELECT * from frame, frame1 WHERE frame1.id = frame.id")
    mysqlframe.show()


    frame.foreachPartition(x => println("分区 ; " + TaskContext.getPartitionId()))
    val endTime = System.currentTimeMillis()
    println(s"========ALL TIME ====== " + (endTime-startTime) )

  }


}
