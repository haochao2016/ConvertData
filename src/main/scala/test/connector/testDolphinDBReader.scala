package test.connector

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType

object testDolphinDBReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DolphinDBConn")
      .master("local[2]")
      .getOrCreate()

    val sd = spark.read.format("com.dolphindb.spark.DolphinDBDataSourceV2")

      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("passwd", "123456")
//      .option("dbPath" , "dfs://chao/TAQ/db")
//      .option("table", "taq")
      .option("tablePath" , "dfs://chao/TAQ/db/taq")
//      .option("tablePath" , "dfs://chao/Spark/db/sparktb1")
      .option("where",  "date=2007.08.10 and symbol='IBM'")
//      .option("where",  "date=2007.08.02 and Symbol = \"A\" and time < 07:07:01")

      .load()

    sd.foreachPartition(part => {
       println( "  分区   " +  TaskContext.getPartitionId())
    })
    val coll = sd.collect
    println( " all Collect   " + coll.length)
    coll.foreach(println(_))



//    sd.createTempView("sd1")
//    val frame = spark.sql("select sum(BIDSIZ) from sd1 group by SYMBOL ")
    /*println("==============frame===============")
    frame.show()
    println("==============frame===============")

    sd.printSchema()
    val sa = sd.select("time")
    sd.show()

    println("=============================")

    sa.show()
    println("============================")*/


    spark.stop()


  }

}
//      .option("columns", "symbol,date,time,bid,ofr")
