package com.dolphindb.ReaderTAQ.comput

import org.apache.spark.sql.SparkSession

object ReaderTAQWithCondition2 {


  def main(args: Array[String]): Unit = {



    val spark = SparkSession.builder().appName("ReaderTAQWithCondition2")
      //      .master("local[2]")
      //      .config("spark.scheduler.mode", "FAIR")
      //      .config("spark.scheduler.pool", "spark-d")
      //      .config("spark.scheduler.allocation.file", "/home/chao/apps/spark/spark-2.3.1-bin-hadoop2.7/conf/fairscheduler.xml")
      .getOrCreate()

    //    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "spark-d")

    val origFrame = spark.read
      //      .option("header", "true")
      //     .parquet(s"hdfs://192.168.1.106:9000/chao/compData/orig/TAQ2007${args(0)}")
      //      .parquet(s"hdfs://cnserver2:9000/chao/compData/TAQ2007${args(0)}")
      .parquet(s"hdfs://cnserver2:9000/chao/dolphindb/TAQ/TAQ2007${args(0)}")

    //     .parquet("D:\\data\\t.csv\\20190827-parquet")

    origFrame.createOrReplaceTempView("TAQ")
    origFrame.printSchema()

    //    b-1
//    val frame = spark.sql("select * from TAQ where SYMBOL ='IBM' and date = to_date('2007-08-10') and TIME >= to_timestamp('2019-04-12 09:30:00')")



    //    b-2
//        val frame =  spark.sql("select SYMBOL, TIME, BID from TAQ where SYMBOL in ('IBM', 'MSFT', 'GOOG', 'YHOO') and" +
//                    " date=to_date('2007-08-10') and TIME between to_timestamp('2019-04-12 09:30:00') and" +
//                    " to_timestamp('2019-04-12 09:30:59') and BID > 0 and OFR > BID")

    //    b-3
//        val frame = spark.sql("select * from TAQ where date = to_date('2007-08-10') and SYMBOL = 'EBAY' ")

    //    b-4
//     val frame = spark.sql("select minute(TIME) as minute " +
//                       " , avg(OFR) as  spread " +
//                       "from TAQ where date= to_date('2007-08-01') and symbol='IBM' and time >= to_timestamp('2019-04-12 09:30:00') and time <= to_timestamp('2019-04-12 16:00:00') " +
//                       "  group by minute ")

    //    b-5
//     val frame =  spark.sql("" +
//       "select minute(TIME) as minute  ," +
//       " (max(OFR)-min(BID)) as gap from TAQ where date=to_date('2007-08-03')and symbol='IBM' and OFR>BID and BID>0 group by SYMBOL, minute" )

    //    b-6
//    val frame =  spark.sql("select DATE , minute(TIME) " +
//            " as minute ,avg(OFR + BID)/ 2 " +
//            " as mid from TAQ where SYMBOL ='IBM' and bid >= 50 and bid <= 90 group by date,  minute")

    val frame = spark.sql("select DATE,OFR, BID from TAQ where symbol ='IBM'")

    //    7   未完成
    /*      val frame0 = spark.sql("select count(*) from   " +
                    "(select (OFR  + 0 + BID) /2.0 as median_mid , SYMBOL from TAQ where date='20070801' and time >= '09:30:00'" +
                    " and  time <= '16:00:00'  ) as tmpTab  group by SYMBOL  ")

        val frame = spark.sql("select (OFR  + 0 + BID) /2.0 as median_mid,  SYMBOL from TAQ order by median_mid")
        frame.count()*/



    //    b-8
//    val frame =  spark.sql("select sum(BID * BIDSIZ)/sum(BIDSIZ) as vwab from TAQ group by DATE, SYMBOL having sum(BIDSIZ) > 0" +
//         "  order by Date desc , SYMBOL  ")



//    frame.foreachPartition(x => println( " partition : " + TaskContext.getPartitionId()))


    val framePer = frame.cache()
    frame.show()

    framePer.createTempView("frame")
    val perRes = spark.sql("select DATE, avg(frame.BID),avg(OFR + BID)/2 as mid from frame  where frame.BID >= 50 group by DATE ")
    perRes.show()
    perRes.printSchema()

    val beginTime = System.currentTimeMillis()
    println("起始时间   ====>   " + beginTime)
    spark.sql("select DATE, avg(frame.BID),avg(OFR + BID)/2 as mid from frame  where frame.BID >= 50 group by DATE").show()

//    + frame.BID

    val endTime  = System.currentTimeMillis()
    println("结束时间   ====>   " +endTime)
    println("所有时间   ====>   " + (endTime-beginTime))
    spark.stop()


  }
}
