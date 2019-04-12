package com.dolphindb.spark.writer

import java.util

import com.xxdb.DBConnection
import com.xxdb.data.{BasicByteVector, BasicDateVector, BasicDoubleVector, BasicIntVector, BasicSecondVector, BasicStringVector, BasicTable, Entity, Utils, Vector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class DolphinDBDataWriter( ip: String,
                           port: Int,
                           user: String,
                           passwd : String,
                           tbPath : String,
                           schema: StructType)
                  extends DataWriter[Row]{

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val conn : DBConnection = new DBConnection
  private var loginFlag = false

  private val names = new util.ArrayList[String]()

  private val originSchema : ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]

  private val dbPath = tbPath.substring(0, tbPath.lastIndexOf("/"))
  private val dbPath_db = "\"" +  dbPath + "\""
  private val table = tbPath.substring(tbPath.lastIndexOf("/") + 1)
  private val table_db = "\""  +  table + "\""

  override def commit(): WriterCommitMessage = null

  override def abort(): Unit = Unit

  override def write(record: Row): Unit = {
    if (!loginFlag) {
      loginFlag = conn.connect(ip, port, user, passwd)
      conn.run(s"db=database($dbPath_db); ${table} = db.loadTable(${table_db})")
      val entity = conn.run(s"schema(${table}).colDefs").asInstanceOf[BasicTable]

//      originSchema = new Array[(String, String)](entity.rows())
      for (i <- 0 until(entity.rows())) {
          names.add(entity.getColumn(0).get(i).getString)
          val nameType = (entity.getColumn(0).get(i).getString, entity.getColumn(1).get(i).getString)
          originSchema += nameType
      }

    }

    val vectors = new util.ArrayList[Vector]()
    for (i <- 0 until(originSchema.size)) {
      originSchema(i)._2 match {
        case "SYMBOL" => vectors.add(new BasicStringVector(Array[String](if (record.getString(i) == null) "" else record.getString(i))))
        case "DATE"  => {
          if (record.getString(i) == null) {
            vectors.add(new BasicDateVector(Array[Int](0, 0, 0)))
          }
          val dates = record.getString(i).split("-")
          vectors.add(new BasicDateVector(Array[Int](Utils.countDays(dates(0).toInt, dates(1).toInt, dates(2).toInt))))
        }
        case "SECOND" => {
          if (record.getString(i) == null) {
            vectors.add(new BasicSecondVector(Array[Int](0, 0, 0)))
          }
          val times = record.getString(i).split(":")
          vectors.add(new BasicSecondVector(Array[Int](Utils.countSeconds(times(0).toInt, times(1).toInt, times(2).toInt))))
        }
        case "DOUBLE" => {
          if (record.getString(i) == null) {
            vectors.add(new BasicDoubleVector(Array[Double](0)))
          }
          vectors.add(new BasicDoubleVector(Array[Double](record.getString(i).toDouble)))
        }
        case "INT" => {
          if (record.getString(i) == null) {
            vectors.add(new BasicIntVector(Array[Int](0)))
          }
          vectors.add(new BasicIntVector(Array[Int](record.getString(i).toInt)))
        }

        case "CHAR" => {
          if (record.getString(i) == null) {
            vectors.add(new BasicByteVector(Array[Byte](0.toByte)))
          }
          vectors.add(new BasicByteVector(Array[Byte](record.getString(i).charAt(0).toByte)))
        }

      }

    }
    val basicTable = new BasicTable(names, vectors)
    val entities = new util.ArrayList[Entity](1)
    entities.add(basicTable)
    conn.run(s"append!{loadTable(${dbPath_db}, ${table_db})}", entities)

  }


}
