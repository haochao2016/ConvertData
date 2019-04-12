/*
package com.dolphindb.conn_ok

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util

import org.apache.orc.DataReader
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory


class JDBCDataSourceV2 extends  DataSourceV2 with ReadSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader = new J2DataSourceReader(options)
}


class J2DataSourceReader(options : DataSourceOptions
                        ) extends  DataSourceReader {


  val url = options.get("url").get()
  val user = options.get("user").get()
  val passwd = options.get("passwd").get()
  val table = options.get("table").get()
  
  private val stringToString: util.Map[String, String] = options.asMap()
  stringToString.containsKey("")


  //此处创建一个Schema 如同在 JDBCRelation 中一样,可以先去访问数据库的Schema 信息
  override def readSchema(): StructType =
    JdbcSchema.resolveTbl(options)

//    ???
  /* StructType(Seq(
    StructField("id", StringType),
    StructField("name", StringType),
    StructField("code", StringType)
  ))*/


  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    import collection.JavaConverters._

    Seq(new J2DataV2ReaderFactory(url, user, passwd, table).asInstanceOf[DataReaderFactory[Row]]).asJava
//                        .asJavaCollec(util.List[DataReaderFactory[Row]])
//      .seqAsJavaList(DataReaderFactory[Row])
  }
}

class J2DataV2ReaderFactory(url : String, user : String, passwd : String ,table : String) extends DataReaderFactory[Row] {

  override def createDataReader(): DataReader[Row] = new J2DataReader(url, user, passwd)

}

class J2DataReader(url : String, user : String, passwd : String )  extends  DataReader[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var conn : Connection = null
  private var rs : ResultSet = null

  override def next(): Boolean = {
    if (rs ==null){
      conn = DriverManager.getConnection(url, user, passwd)
      val sqlbuilder = new StringBuilder()
      sqlbuilder.append(s"select * from mysqlConn ")
      logger.info(sqlbuilder.toString())

      val stmt = conn.prepareStatement(sqlbuilder.toString())
      rs = stmt.executeQuery()

      println("==============================================================")
      val meta = rs.getMetaData()
      println(meta)

      println("==============================================================")
    }
    rs.next()
  }


  override def get(): Row = {
    val value = (  rs.getString(1),
      rs.getString(2), rs.getString(3))
    Row.fromTuple(value)
  }

  override def close(): Unit = {
    conn.close()
  }
}

object J2ExampleTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MysqlTestConn")
      .master("local[2]")
      .getOrCreate()


    import  spark.implicits._

   val sparkframe =  spark.read.format("com.dolphindb.conn_ok.JDBCDataSourceV2")
     .schema(StructType(Seq(
       StructField("id", StringType),
       StructField("name", StringType),
       StructField("code", StringType)
     )))
      .option("user","root")
      .option("url" , "jdbc:mysql://115.239.209.189:3306/spark-conn")
      .option("passwd", "123456")
      .option("table", "mysqlConn")
      .load()
     //.select("id")
//     .sort($"code".desc)



//     val sparkset = sparkframe.as[mc]
//     sparkset.sort($"code".desc).show()
//     println( "      =============count all===  " + sparkframe.collect().length )

    sparkframe.printSchema()
    sparkframe.show()

  }

}

case class mc (id : String, name: String , code : String)


*/
