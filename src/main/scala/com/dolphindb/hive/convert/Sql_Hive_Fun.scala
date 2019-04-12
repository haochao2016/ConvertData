package com.dolphindb.hive.convert

import org.apache.spark.sql.SparkSession

object Sql_Hive_Fun {

  def main(args: Array[String]): Unit = {

    val beginTime = System.currentTimeMillis()
    println("    =======起始时间=====   " + beginTime)


    val spark = SparkSession.builder().appName("HIVE_DATA")
//      .master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://chao/hive/warehouse")
      .getOrCreate();
    import spark.implicits._

    //===================================================
//    val frame = spark.sql("select SYMBOL, EX, count(EX) from TAQ group by SYMBOL,EX")
    //===================================================

//    val frame = spark.sql("select count(*) from taq where date='20070801'")
//    val frame = spark.sql("select * from taq")

//    ====================================================================================

//    a-1
    val frame = spark.sql("select SYMBOL, count(EX) from TAQ where date='20070801' group by SYMBOL")


//    ======================================================================================
//    b-1
//    val frame = spark.sql("select * from TAQ where SYMBOL ='IBM' and date = '20070810' and TIME >= '09:30:00'")
//      b-2
//      val frame =  spark.sql("select SYMBOL, TIME, BID from TAQ where SYMBOL in ('IBM', 'MSFT', 'GOOG', 'YHOO') and" +
//                           " date='20070810' and TIME between '09:30:00' and '09:30:59' and BID > 0 and OFR > BID")

//    b-3
//    val frame = spark.sql("select * from TAQ where date = '20070810' and SYMBOL = 'EBAY' order by (OFR-BID) desc ")
//    b-4
     /* val frame = spark.sql("select (SUBSTRING(TIME, 1 , POSITION(':' in TIME)-1) * 60  + SUBSTRING(TIME, POSITION(':' in TIME)+1 , 2)) as minute " +
        " , (OFR + BID)/(OFR-BID) as  spread " +
        "from TAQ where date='20070801' and time >= '09:30:00' and time <= '16:00:00' " +
        " and OFR >BID and BID>0 ")*/

//    b-5
     /* val frame =  spark.sql("select SUBSTRING(TIME, 1 , POSITION(':' in TIME)-1) * 60  + SUBSTRING(TIME, POSITION(':' in TIME)+1 , 2) as minute ," +
        " (max(OFR)-min(BID)) as gap from TAQ where date='20010803' and OFR>BID and BID>0 group by SYMBOL, minute" +
        "")*/


    //    b-6
   /* val frame =  spark.sql("select DATE , SUBSTRING(TIME, 1 , POSITION(':' in TIME)-1) * 60  + SUBSTRING(TIME, POSITION(':' in TIME)+1 , 2)" +
      " as minute ,avg(OFR + BID)/ 2 " +
      " as mid from TAQ where SYMBOL ='IBM' and time >= '09:30:00' and time <= '16:00:00' group by date,  minute")*/

//    b-8
//    val frame =  spark.sql("select sum(BID * BIDSIZ)/sum(BIDSIZ) as vwab from TAQ group by DATE, SYMBOL having sum(BIDSIZ) > 0" +
//      "  order by Date desc , SYMBOL   ")





    println("=============================================")
    frame.show()
    println(frame.collect().length)
    println("=============================================")



    val endTime = System.currentTimeMillis()
    println("    =======结束时间=====   " + endTime)
    println("    =======所有时间=====   " + (endTime - beginTime ))

    spark.stop()

  }

}
