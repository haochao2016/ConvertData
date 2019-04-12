package com.dolphindb.spark.reader

import java.util

import com.dolphindb.spark.reader.schema.DolphinDBSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

class DolphinDBDataSourceV2Reader(options: DataSourceOptions)
      extends  DataSourceReader{

  override def readSchema(): StructType = DolphinDBSchema.resolveTable(options)

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._

    Seq{new DolphinDBDataV2ReaderFactory(options).asInstanceOf[DataReaderFactory[Row]]}.asJava


  }


}
