package com.dolphindb.spark.writer

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class DolphinDBDataV2WriterFactory(options : DataSourceOptions,
                                   schema: StructType)
                          extends DataWriterFactory[Row]{

//  val ip = options.get("ip").get()
//  val port = options.getInt("port", 0)
//  val user = options.get("user").get()
//  val passwd = options.get("passwd").get()
//  val table = options.get("table").get()
//  val dbPath = options.get("dbPath").get()

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



  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] =
        new DolphinDBDataWriter(ip, port, user, passwd, tbPath, schema)

}
