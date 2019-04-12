package com.dolphindb.spark.reader

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

class DolphinDBDataV2ReaderFactory(options : DataSourceOptions) extends DataReaderFactory[Row]{

  private val paramMap: util.Map[String, String] = options.asMap()

  private val ip = options.get("ip").get()
  private val port = options.getInt("port", 0)
  private val user = options.get("user").get()
  private val passwd = options.get("passwd").get()

  private val columns = if (paramMap.containsKey("columns")) paramMap.get("columns") else  ""
  private val where = if (!paramMap.containsKey("where")) "" else " where " + paramMap.get("where")
  private val sql = if (paramMap.containsKey("sql")) paramMap.get("sql") else ""
  private val tbPath = options.get("tablePath").get()



  override def createDataReader(): DataReader[Row] = new DolphinDBDataReader(
    ip, port, user, passwd , tbPath, columns, where, sql
  )

}
