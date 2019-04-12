package com.dolphindb.spark.reader.example.refer2

import java.util
import java.util.Optional

import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode}

class CustomDataSourceV2  extends DataSourceV2
          with ReadSupport  with WriteSupport
          {
            override def createReader(options: DataSourceOptions): DataSourceReader = new CustomDataSourceV2Reader(options)

            override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???
          }

class CustomDataSourceV2Reader(options: DataSourceOptions) extends DataSourceReader {

  override def readSchema(): StructType = ???

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new CustomDataSourceV2ReaderFactory().asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}

class CustomDataSourceV2ReaderFactory extends  DataReaderFactory[Row] {


  override def createDataReader(): DataReader[Row] = new CustomDataReader
}

class CustomDataReader extends DataReader[Row] {


  override def next(): Boolean = ???

  override def get(): Row = ???

  override def close(): Unit = ???
}


class CustomDataSourceV2Writer extends DataSourceWriter {



  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def createWriterFactory(): DataWriterFactory[Row] = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???

}

class CustomDataWriterFactory extends DataWriterFactory[Row] {


  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = ???
}


class CustomDataWriter extends DataWriter[Row] {


  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def write(record: Row): Unit = ???
}



