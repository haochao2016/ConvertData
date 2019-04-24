package com.dolphindb.ReaderTAQ.comput

import org.apache.spark.sql.SparkSession

object ReaderHiveTAQWithCondition2 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("ReaderHiveTAQWithCondition2")
      //      .master("local[2]")
      //      .config("spark.scheduler.mode", "FAIR")
      //      .config("spark.scheduler.pool", "spark-d")
      //      .config("spark.scheduler.allocation.file", "/home/chao/apps/spark/spark-2.3.1-bin-hadoop2.7/conf/fairscheduler.xml")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://chao/hive/warehouse")
      .getOrCreate()


    //    ======================================================================================
    //    b-1
//    val frame = spark.sql("select * from TAQ where SYMBOL ='IBM' and date = to_date('2007-08-10')" +
//                                " and TIME >= to_timestamp('2007-08-10 09:30:00')")

    //      b-2
//          val frame =  spark.sql("select SYMBOL, TIME, BID from TAQ where SYMBOL in ('IBM', 'MSFT', 'GOOG', 'YHOO') and date = " +
//                  " to_date('2007-08-10') and TIME between to_timestamp('2007-08-10 09:30:00') and to_timestamp('2007-08-10 09:30:59') and BID > 0 and OFR > BID")

    //    b-3
//        val frame = spark.sql("select * from TAQ where date = to_date('2007-08-10') and SYMBOL = 'EBAY' order by (OFR-BID) desc ")
    //    b-4
//     val frame = spark.sql("select minute(time) as minute " +
//       " , avg(OFR) as  spread " +
//       "from TAQ where date= to_date('2007-08-01') and symbol='IBM' and time >= to_timestamp('2007-08-01 09:30:00') and time <= to_timestamp('2007-08-01 16:00:00') " +
//       " group by minute ")

    //    b-5
//     val frame =  spark.sql("select minute(time) as minute  ," +
//       " (max(OFR)-min(BID)) as gap from TAQ where date= to_date('2007-08-03') and symbol='IBM'  and OFR>BID and BID>0 group by SYMBOL, minute"  )


    //    b-6
//     val frame =  spark.sql("select DATE , minute(time) as minute ," +
//       " avg(OFR + BID)/ 2 " +
//       " as mid from TAQ where SYMBOL ='IBM' and bid >= 50 and bid <= 90 group by date,  minute")

    val frame = spark.sql("select DATE,OFR,BID from TAQ where symbol ='IBM'")

    //    b-8
//        val frame =  spark.sql("select sum(BID * BIDSIZ)/sum(BIDSIZ) as vwab from TAQ group by DATE, SYMBOL having sum(BIDSIZ) > 0" +
//          "  order by Date desc , SYMBOL   ")

    val framePer = frame.cache()
    frame.show()

    framePer.createTempView("frame")
    val perRes = spark.sql("select avg(frame.BID),max(ofr) as mid from frame  where frame.BID >= 50 group by DATE order by (frame.OFR-frame.BID) desc")
    perRes.printSchema()
    perRes.show()

    val beginTime = System.currentTimeMillis()
    println("起始时间   ====>   " + beginTime)
    spark.sql("select avg(frame.BID), max(ofr) as mid from frame  where frame.BID >= 50 group by DATE order by (frame.OFR-frame.BID) desc").show()




    val endTime = System.currentTimeMillis()
    println("    =======结束时间=====   " + endTime)
    println("    =======所有时间=====   " + (endTime - beginTime ))

    spark.stop()

  }

}
