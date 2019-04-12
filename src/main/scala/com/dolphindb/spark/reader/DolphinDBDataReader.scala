package com.dolphindb.spark.reader


import java.sql.{Date, Timestamp}

import com.dolphindb.spark.exception.{DolphinDBException, TableException}
import com.dolphindb.spark.reader.schema.DolphinDBSchema
import com.xxdb.DBConnection
import com.xxdb.data.{BasicTable, Utils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class DolphinDBDataReader( ip : String, port : Int, user : String, passwd : String,
                           tbPath : String, columns : String, where : String,
                           sql : String
                         )
              extends DataReader[Row]{


  /**  example:
    * {{{
    *   [(dbpath, tbname)]
    * }}}
    */
  private val tbPathTupArr = {
    val tbPathArr = tbPath.split(",")
    val tbPathTupArrTmp = new Array[(String, String)](tbPathArr.length)
    for (i <- 0 until tbPathArr.length) {
      tbPathTupArrTmp(i) = (tbPathArr(i).substring(0, tbPathArr(i).lastIndexOf("/")),
          tbPathArr(i).substring(tbPathArr(i).lastIndexOf("/")+1, tbPathArr(i).length))
    }
    tbPathTupArrTmp
  }
  private val tableNames = new Array[String](tbPathTupArr.length)
  private val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * DolphinDB Connection
    */
  private val conn : DBConnection = new DBConnection
  /**
    * DolphinDB Connection Flag
    */
  private var loginFlag = false
  private var tableLength : Long = 0
  private var cursor = 0
  private var sqlTable : BasicTable = null
  private val cols = if (columns.equals("")) "*" else columns
  private val colSize = if(columns.equals("")) DolphinDBSchema.tblColSize else columns.split(",").length

  /**
    *  Determine whether there is the next data
    * @return true or false
    */
  override def next(): Boolean = {
    if (!loginFlag) {
      loginFlag = conn.connect(ip, port, user, passwd)  // login
      this.loadTable()
      if (!sql.equals("")) {   /**sql目前为""*/
        val fromIndex = sql.indexOf("from")
        val countTmpSql = "select count(*)" + sql.substring(fromIndex, sql.length)
        logger.info(sql)

        /**
          *  Get the length of the user-specified SQL result
           */
        tableLength = conn.run(countTmpSql).asInstanceOf[BasicTable].getColumn(0).get(0).toString.toLong
        sqlTable = conn.run(sql).asInstanceOf[BasicTable]
      } else {
        if (tbPathTupArr.length != 1) {
          throw new TableException("There can only be one table without specifying SQL ")
        }

        tableLength = conn.run(s"select count(*) from ${tableNames(0)} ${where} ").asInstanceOf[BasicTable].
              getColumn(0).get(0).toString.toLong

        val dbSql = s"select ${cols} from ${tableNames(0)} ${where} "
        sqlTable = conn.run(dbSql).asInstanceOf[BasicTable]
      }
    }
    if (cursor < tableLength)  true else false
  }

  /**
    *  Get the next data
    * @return
    */
  //是否返回Row最佳?
  override def get(): Row = {
    val rowArr = new ArrayBuffer[Any]()
//    println(colSize)
    for (i <- 0 until(colSize)) {
      val fieldType = DolphinDBSchema.schema(i).dataType
      val fieldOriginType = DolphinDBSchema.originType(i)
      val dataWithType = applyTypeInData(fieldType, sqlTable.getColumn(i).get(cursor).toString, fieldOriginType)
      rowArr += dataWithType
    }
    cursor += 1
    Row.fromSeq(rowArr)
  }

  override def close(): Unit = {
    conn.close()
  }

  /**
    * load Table
    */
  def loadTable(): Unit = {
    for (i <- 0 until tbPathTupArr.length) {
      val dbPath_db = "\"" + tbPathTupArr(i)._1 + "\""
      val table_db = "\"" + tbPathTupArr(i)._2 + "\""
      val dbName = tbPathTupArr(i)._1.replace("/", "_").substring(1).replace(":","")
      val tbName = tbPathTupArr(i)._2
      tableNames(i) = tbPathTupArr(i)._2
      conn.run(s"${dbName}= database(${dbPath_db})")
      conn.run(s"${tbName} =${dbName}.loadTable(${table_db})")
    }
  }

  /**
    *  Converting data to corresponding types
    *  Convert DolphinDB dataType to Spark dataType
    * @param fieldType  data type in spark
    * @param fieldVal  data value
    * @param fieldOriginType  data type in DolphinDB
    * @return
    */
 def applyTypeInData(fieldType : DataType= StringType, fieldVal : String, fieldOriginType: String) : Any = fieldType match {

   case DateType => Date.valueOf(fieldVal.replace(".", "-"))
   case IntegerType => {
     fieldOriginType match {
       case "MONTH" => Utils.countMonths(fieldVal.split(".")(0).toInt,
         fieldVal.split(".")(1).split("M")(0).toInt)
       case "TIME" => {
         val time = fieldVal.split(":")
         Utils.countMilliseconds(time(0).toInt, time(1).toInt, time(2).split(".")(0).toInt, time(2).split(".")(1).toInt)
       }
       case "MINUTE" => Utils.countMinutes(fieldVal.split(":")(0).toInt,
         fieldVal.split(":")(1).split("m")(0).toInt)
       case "SECOND" => Utils.countSeconds(fieldVal.split(":")(0).toInt,
         fieldVal.split(":")(1).toInt, fieldVal.split(":")(2).toInt)
       case _ => fieldVal.toInt
     }
   }
   case TimestampType => Timestamp.valueOf(fieldVal.replace("T" ," "))
   case NullType => null
   case BooleanType => if (fieldVal.equals("0") || fieldVal.toLowerCase().equals("false")) false else true
   case DoubleType => fieldVal.toDouble
   case FloatType => fieldVal.toFloat
   case LongType => fieldVal.toLong
   case ShortType => fieldVal.toShort
   case ByteType => fieldVal.charAt(0).toByte
   case _ => fieldVal
 }


}
