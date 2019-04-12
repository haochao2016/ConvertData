package com.dolphindb.spark.reader.schema

import com.dolphindb.spark.exception.{NoDataBaseException, TableException}
import com.xxdb.DBConnection
import com.xxdb.data.{BasicTable }
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


object DolphinDBSchema extends Logging{

  var tblColSize = 0
  var schema : StructType= null
  val originType : ArrayBuffer[String] = new ArrayBuffer[String]()

  def resolveTable(options: DataSourceOptions): StructType = {
    val optionsMap = options.asMap()
      //校验参数是否正确,具有ip,port,user, passwd,table,
    val ip = options.get("ip").get()
    val port = options.getInt("port", 0)
    val user = options.get("user").get()
    val passwd = options.get("passwd").get()
    val sql = if (optionsMap.containsKey("sql")) options.get("sql").get() else ""


    /** ALL DolphinDB_Path & DolphinDB_Table
      *  An example :
      *  {{{
      *   [("dfs://dolphin/db", "tb1"),
      *   ("dfs://dolphin/db", "tb2")]
      *  }}}
      * */
    val tbPathTupArr = {
      val tbPathArr = options.get("tablePath").get().split(",")
      if (sql.equals("") && tbPathArr.length != 1 ) {
          throw new TableException("There can only be one table without specifying SQL ")
      } else {
        val tbPathTupArrTmp = new Array[(String, String)](tbPathArr.length)
        for (i <- 0 until tbPathArr.length) {
          tbPathTupArrTmp(i) = (tbPathArr(i).substring(0, tbPathArr(i).lastIndexOf("/")),
            tbPathArr(i).substring(tbPathArr(i).lastIndexOf("/") + 1, tbPathArr(i).length))
        }
        tbPathTupArrTmp
      }
    }

    /**
      * ALL TABLE NAME
      */
    val tableNames = new Array[String](tbPathTupArr.length)
    val conn = new DBConnection()
    conn.connect(ip, port , user, passwd)

    /**   Load Table
      * */
    for (i <- 0 until tbPathTupArr.length) {
      val dbPath_db = "\"" + tbPathTupArr(i)._1 + "\""
      val table_db = "\"" + tbPathTupArr(i)._2 + "\""
      val dbName = tbPathTupArr(i)._1.replace("/", "_").substring(1).replace(":","")
      val tbName = tbPathTupArr(i)._2
      tableNames(i) = tbPathTupArr(i)._2
      if (conn.run(s"existsDatabase(${dbPath_db})") == null ) {
        logError(s"No Database ${tbPathTupArr(i)._1}")
        throw new NoDataBaseException(s"No DataBase : ${tbPathTupArr(i)._1}")
      }
      conn.run(s"${dbName}=database(${dbPath_db})")
      conn.run(s"${tbName}=${dbName}.loadTable(${table_db})")
    }


    /**   Get Schema
      *    There are two situations.
      *    Users have input a DolphinDB SQL or Not
       */
    var schemaTbl : BasicTable = null
    /**
      *  Array contains name -> type
      * An example :
      * {{{
      *  [(id : String),
      *   (name : String)]
      *   }}}
      * */
    val structArr = new ArrayBuffer[StructField]()
      // Users have not specified SQL
    if (tbPathTupArr.length == 1) {
      val schemaStr = tableNames(0)
      logInfo(s" table Name is ${schemaStr}" )
      schemaTbl = conn.run(s"schema(${schemaStr}).colDefs").asInstanceOf[BasicTable]
      tblColSize = schemaTbl.rows()
      for (i <- 0 until(tblColSize)) {
        val struct = convertToStructField(schemaTbl.getColumn(0).get(i).getString, schemaTbl.getColumn(1).get(i).getString)
        originType += schemaTbl.getColumn(1).get(i).getString
        structArr += struct
      }
    } else {
      // Users have specified SQL
      // 用户指定SQL 查询语句的时候运行
   /*  val tableTmpName  new ArrayBuffer[String]()
      var sqlTblNameTmp : mutable.StringBuilder =  new mutable.StringBuilder()
      for (i <- 0 until(tbPathTupArr.length)) {
        sqlTblNameTmp.append(tbPathTupArr(i)._1)
        sqlTblNameTmp.append("_")
        sqlTblNameTmp.append(tbPathTupArr(i)._2)
        sqlTblNameTmp.append("-")
        val partiSchemaVal = conn.run(s"schema(${tableNames(i)}).partitionSchema").asInstanceOf[Vector]
        val partitionColumnName =  conn.run(s"schema(${tableNames(i)}).partitionColumnName").asInstanceOf[Scalar]
        conn.run(s"${tableNames(i)}_Temp${i} = select * from ${tableNames(i)} where ${partitionColumnName} = ${partiSchemaVal}")
        tableTmpName.+= (s"${tableNames(i)}_Temp${i}")

      }
      val sqlTblName = sqlTblNameTmp.substring(0, sqlTblNameTmp.indexOf("-"))
     */

    }

      schema = StructType(structArr.toSeq)
      schema
  }


  private def convertToStructField (originName : String ,
                               originType : String) : StructField =  originType match {

    case "SYMBOL" => StructField(originName, StringType)
    case "STRING" => StructField(originName, StringType)

    case "DATE" => StructField(originName, DateType)
    case "MONTH" => StructField(originName, IntegerType)
    case "TIME" => StructField(originName, IntegerType)
    case "MINUTE" => StructField(originName, IntegerType)
    case "SECOND" => StructField(originName, IntegerType)
    case "DATETIME" => StructField(originName, TimestampType)

    case "VOID" => StructField(originName, NullType)
    case "BOOL" => StructField(originName, BooleanType)
    case "DOUBLE" => StructField(originName, DoubleType)
    case "FLOAT" => StructField(originName, FloatType)
    case "LONG" => StructField(originName, LongType)
    case "INT" => StructField(originName, IntegerType)
    case "SHORT" => StructField(originName, ShortType)
    case "CHAR" => StructField(originName, ByteType)
    case "" => StructField(originName, StringType)

  }



}

////////////////////////////////////////////////TEST////////////////////////////////////////////////

object testDbConn {

  def main(args: Array[String]): Unit = {
    val ip= "115.239.209.224"
    val port = 16961
    val user = "admin"
    val passwd = "123456"
    val dbPath ="\"dfs://chao/TAQ/db\""
    val table = "\"taq\""


    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port , user, passwd)


    val existDB =  s"existsDatabase($dbPath)"
    if (conn.run(existDB) == null) {
      throw new Exception(s"There'is no database named ${dbPath}")
    }
    else {
      val dben = conn.run(s"db=database($dbPath)")
      val stab = s"taq1 = db.loadTable($table);schema(taq1).colDefs"
      val tblen = conn.run(stab).asInstanceOf[BasicTable]
      val size = tblen.getColumn(0).rows()
      val nameDB = new Array[String](size)
      val typeDB = new Array[String](size)

//      util.HashMap
//      new immutable.HashMap()


      for ( i <- 0 until size) {
        nameDB(i) = tblen.getColumn(0).get(i).getString
        typeDB(i) = tblen.getColumn(1).get(i).getString
      }
      val tuples = nameDB.zip(typeDB)
      println(nameDB)
      println(typeDB)

      println("==============")


      val sqldata = s"select bid , time from taq1 where date=2007.08.10, symbol='IBM'"
       val tabledata = conn.run(sqldata).asInstanceOf[BasicTable]

      println(tabledata)

      val nameDS = tabledata.getColumnName(1)

      println( " nameDS  "  +   nameDS)
      val dataType = tabledata.getDataType
      println("  dataType   "      +  dataType)
      val timedata = tabledata.getColumn("time")
      println( "timedata"   +   timedata)

      val DataForm =  tabledata.getDataForm
      println("DataForm"   +   tabledata.getDataForm)

      val DataCategory = tabledata.getDataCategory
      println( "DataCategory" +  tabledata.getDataCategory)

      val column1 = tabledata.getColumn(0)

      println(   " column1 "         + column1.get(1))
      println(   "  column1    " +  column1)
      println("===================")
      val iterator = Iterator(tabledata)
      while (iterator.hasNext) {
        println(iterator.next())
      }

      val sqldata1 = s"select count(*) from taq1 where date=2007.08.10, symbol='IBM'"
      val tabledata1 = conn.run(sqldata1).asInstanceOf[BasicTable]
      println("  tabledata1   " + tabledata1)

      println("  tabledata1  val " + tabledata1.getColumn(0).get(0))
    }

  }




}
