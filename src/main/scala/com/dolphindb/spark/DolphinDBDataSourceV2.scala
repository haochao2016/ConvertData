package com.dolphindb.spark

import java.util.Optional

import com.dolphindb.spark.reader.DolphinDBDataSourceV2Reader
import com.dolphindb.spark.writer.DolphinDBDataSourceV2Writer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
  *  DolphinDB implements
  *
  */
class DolphinDBDataSourceV2
            extends DataSourceV2
            with ReadSupport
            with WriteSupport
 {
      override def createReader(options: DataSourceOptions): DataSourceReader = new DolphinDBDataSourceV2Reader(options)

   override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions)
                                      : Optional[DataSourceWriter] = Optional.of(new DolphinDBDataSourceV2Writer(options, schema, mode))
 }

