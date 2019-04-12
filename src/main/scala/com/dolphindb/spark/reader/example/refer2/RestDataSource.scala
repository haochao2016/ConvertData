/*
package v2.refer.refer2

import java.util
import java.util.Optional

import com.alibaba.fastjson.{JSONArray, JSONObject, JSONPath}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.v2.DataSourceV2


class RestDataSource extends DataSourceV2 with
            ReadSupport
            with WriteSupport {


  override def createReader(options: DataSourceOptions): DataSourceReader =
    new RestDataSourceReader(
      options.get("url").get(),
      options.get("params").get(),
      options.get("xPath").get(),
      options.get("schema").get()

    )

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???
}

class RestDataSourceReader(url: String, params: String, xPath: String, schemaString: String)
      extends  DataSourceReader {

  var requiredSchema = StructType.fromDDL(schemaString)

  override def readSchema(): StructType = requiredSchema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import  collection.JavaConverters._
    Seq( new RestDataReaderFactory(url, params, xPath).asInstanceOf[DataReaderFactory[Row]]).asJava

  }
}

class RestDataReaderFactory(url: String, params: String, xPath: String) extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] =  new RestDataReader(url, params, xPath)
}

class RestDataReader(url: String, params: String, xPath: String) extends  DataReader[Row] {

  val data : Iterator[Seq[AnyRef]] = getIterator

  override def next(): Boolean = ???

  override def get(): Row = {
    val seq = data.next().map{
      case decimal : BigDecimal => decimal.doubleValue()
      case x => x
    }
    Row(seq :  _* )
  }

  override def close(): Unit = {
    println("close source")
  }

  def getIterator : Iterator[Seq[AnyRef]] = {
    import scala.collection.JavaConverters._
    val res : List[AnyRef] = RestDataSource.requestData(url, params, xPath)
    res.map(r => {
      r.asInstanceOf[JSONObject].asScala.values.toList
    }).toIterator
  }

}

class RestDataSourceWriter extends DataSourceWriter {


  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def createWriterFactory(): DataWriterFactory[Row] = new RestDataWriterFactory

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}

class RestDataWriterFactory extends  DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] =

           new RestDataWriter(partitionId, attemptNumber)

}

class RestDataWriter(partitionId: Int, attemptNumber: Int) extends DataWriter[Row] {
  override def commit(): WriterCommitMessage = {
    RestWriterCommitMessage(partitionId, attemptNumber)
  }

  override def abort(): Unit = {
    println("abort 方法被出发了")
  }

  override def write(record: Row): Unit = {
    println(record)
  }
}




case class RestWriterCommitMessage(partitionId: Int, attemptNumber: Int) extends WriterCommitMessage

object RestDataSource {

  def requestData(url: String, params: String, xPath: String): List[AnyRef] = {
    import scala.collection.JavaConverters._
//    val response = Request.Post(url).bodyString(params, ContentType.APPLICATION_JSON).execute()
//    JSONPath.read(response.returnContent().asString(), xPath)
//      .asInstanceOf[JSONArray].asScala.toList
  }
}


object RestDataSourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read
      .format("com.hollysys.spark.sql.datasource.rest.RestDataSource")
      .option("url", "http://model-opcua-hollysysdigital-test.hiacloud.net.cn/aggquery/query/queryPointHistoryData")
      .option("params", "{\n    \"startTime\": \"1543887720000\",\n    \"endTime\": \"1543891320000\",\n    \"maxSizePerNode\": 1000,\n    \"nodes\": [\n        {\n            \"uri\": \"/SymLink-10000012030100000-device/5c174da007a54e0001035ddd\"\n        }\n    ]\n}")
      .option("xPath", "$.result.historyData")
      //`response` ARRAY<STRUCT<`historyData`:ARRAY<STRUCT<`s`:INT,`t`:LONG,`v`:FLOAT>>>>
      .option("schema", "`s` INT,`t` LONG,`v` DOUBLE")
      .load()


    df.printSchema()
    df.show(false)
    //    df.repartition(5).write.format("com.hollysys.spark.sql.datasource.rest.RestDataSource")
    //      .save()
  }
}















*/
