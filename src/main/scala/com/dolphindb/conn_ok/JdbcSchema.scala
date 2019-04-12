package com.dolphindb.conn_ok

import java.sql.{DriverManager, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{MetadataBuilder, StringType}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.{StructField, StructType}

object JdbcSchema extends Logging{


  def resolveTbl (options : DataSourceOptions) : StructType = {

    val url = options.get("url").get()
    val table = options.get("table").get()
    val user = options.get("user").get()
    val passwd = options.get("passwd").get()

    val dialect = JdbcDialects.get(url)


    //    Class.forName()
    val conn = DriverManager.getConnection(url, user, passwd)
    val stat = conn.prepareStatement(s"select *  from $table where 1=0")
    val rs = stat.executeQuery()
    val rsmd = rs.getMetaData
    val ncols = rsmd.getColumnCount
//    val sf = new ArrayOps()
    val fields = new Array[StructField](ncols);
    var i = 0;
    while (i < ncols) {
      i += 1;
      val columeName = rsmd.getColumnLabel(i)
      val dataType = rsmd.getColumnType(i)
      val typeName = rsmd.getColumnTypeName(i)
      val fieldSize = rsmd.getPrecision(i)
      val fieldScale = rsmd.getScale(i)
      val nullable = rsmd.isNullable(i)

//      val isSigned = {
//        try {
//          rsmd.isSigned(i)
//        } catch {
//          // Workaround for HIVE-14684:
//          case e: SQLException if
//          e.getMessage == "Method not supported" &&
//            rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" => true
//        }
//      }
      val metadata = new MetadataBuilder().putLong("scale", fieldScale)
//      val columnType =
//        dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
         // getCatalystType(dataType, fieldSize, fieldScale, isSigned))
      fields(i-1) = StructField(columeName, StringType ,/*if(nullable ==0 ) true else*/ true)

    }
    conn.close()
    val schema = new StructType(fields)
    schema

  }

}
