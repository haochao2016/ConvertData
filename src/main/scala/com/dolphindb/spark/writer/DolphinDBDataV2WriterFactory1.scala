package com.dolphindb.spark.writer

import java.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class DolphinDBDataV2WriterFactory1  (options : DataSourceOptions, schema: StructType)
extends DataWriterFactory[InternalRow]{

  //=========================================================

  private val paramMap: util.Map[String, String] = options.asMap()


  private val ip = options.get("ip").get()
  private val port = options.getInt("port", 0)
  private val user = options.get("user").get()
  private val passwd = options.get("passwd").get()

  //  private val columns = if (paramMap.containsKey("columns")) paramMap.get("columns") else  ""
  //  private val where = if (!paramMap.containsKey("where")) "" else " where " + paramMap.get("where")
  //  private val sql = if (paramMap.containsKey("sql")) paramMap.get("sql") else ""
  private val tbPath = options.get("tablePath").get()



  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] =
               new DolphinDBDataWriter1(ip, port, user, passwd, tbPath, schema)

}
