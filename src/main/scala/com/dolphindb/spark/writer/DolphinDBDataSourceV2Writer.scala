package com.dolphindb.spark.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Column, Row, SaveMode}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class DolphinDBDataSourceV2Writer(options : DataSourceOptions,
                                  schema: StructType,
                                  mode: SaveMode
                                 ) extends
            DataSourceWriter{

  override def commit(messages: Array[WriterCommitMessage]): Unit = Unit

  override def createWriterFactory(): DataWriterFactory[Row] =
      new DolphinDBDataV2WriterFactory(options, schema)

//  override def createWriterFactory(): DataWriterFactory[InternalRow] =
//      new DolphinDBDataV2WriterFactory1(options, schema)

  override def abort(messages: Array[WriterCommitMessage]): Unit = Unit

}
