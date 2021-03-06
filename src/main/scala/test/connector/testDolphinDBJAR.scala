package test.connector

import java.time.LocalDate
import java.util

import com.xxdb.DBConnection
import com.xxdb.data.{BasicByteVector, BasicChart, BasicDate, BasicDateVector, BasicDoubleVector, BasicInt, BasicIntVector, BasicSecondVector, BasicStringVector, BasicTable, Entity, Utils, Vector}


object testDolphinDBJAR {


  def main(args: Array[String]): Unit = {
    val ip = "115.239.209.224"
    val port = 16961
    val user = "admin"
    val passwd = "123456"
    val dbPath = "\"dfs://chao/TAQ/db\""
    val table = "\"taq\""


    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)
    val conn1 = new DBConnection()
    conn1.connect(ip, port, user, passwd)

        readerDB(conn, dbPath, table)
//    new testDolphinDBJARRE().readerDB(conn, dbPath, table)


//    writer()

  }


  def writer() = {
    val ip = "115.239.209.224"
    val port = 16961
    val user = "admin"
    val passwd = "123456"
    val dbPath = "\"dfs://chao/Spark/db\""
    val table = "\"sparktb1\""
    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)
    val conn1 = new DBConnection()
    conn1.connect(ip, port, user, passwd)

    val existDB = s"existsDatabase($dbPath)"
    if (conn.run(existDB) == null) {
      throw new Exception(s"There'is no database named ${dbPath}")
    } else {
      try {
        conn.run(s"db=database($dbPath); sparktb1 = db.loadTable($table)")

      } catch {
        case e : java.io.IOException => throw  new Exception("hsidf")
      }

      val symbol = Array[String]("A","A","A","A", "B")
//      val date = Array[Int](20070804,20070804,20070804,20070804,20070804)
//      new BasicDate(Utils.countDays(2007, 8, 4))
//      LocalDate.of(2007, 8, 4)
      val date = Array[Int](Utils.countDays(2007, 8, 4),
                            Utils.countDays(2007, 8, 4),
                            Utils.countDays(2007, 8, 4),
                            Utils.countDays(2007, 8, 4),
                            Utils.countDays(2007, 8, 4))

      val time = Array[Int](162434,162434,162434,162434,162434 )

      val bid = Array[Double](6,6,6,6,9)
      val ofr = Array[Double](6,6,6,6,9)
      val bidsiz = Array[Int](2,3,4,5,9)
      val ofrsiz = Array[Int](2,3,4,5,9)
      val mode = Array[Int](2,3,4,5,9)
//      val mode = Array[Int](2,3,4,5)
      val ex = Array[Byte](12 ,56,56,2,5)
      val mmid = Array[String]("2","5","7","34","27689")

      val args : util.List[Vector] =  util.Arrays.asList[Vector](new BasicStringVector(symbol), new BasicDateVector(date), new BasicSecondVector(time),
        new BasicDoubleVector(bid), new BasicDoubleVector(ofr), new BasicIntVector(bidsiz), new BasicIntVector(ofrsiz),
        new BasicIntVector(mode), new BasicByteVector(ex), new BasicStringVector(mmid))
      val list : util.List[String]= new util.ArrayList[String]()
      list.add("symbol")
      list.add("date")
      list.add("time")
      list.add("bid")
      list.add("ofr")
      list.add("bidsiz")
      list.add("ofrsiz")
      list.add("mode")
      list.add("ex")
      list.add("mmid")
      val table1 = new BasicTable(list, args)
      val entities = new util.ArrayList[Entity](1)
      entities.add(table1)
      conn.run(s"append!{loadTable(${dbPath}, 'sparktb1')}",  entities)
      println("=======")
    }



  }

  def readerDB(conn : DBConnection, dbPath : String,table : String ) : Unit = {



    val existDB = s"existsDatabase($dbPath)"
    if (conn.run(existDB) == null) {
      throw new Exception(s"There'is no database named ${dbPath}")
    }
    else {
      val dben = conn.run(s"db=database($dbPath)")

    try {
      val loader = conn.run(s"db.loadTable($table)")
      true
    } catch {
      case e : java.io.IOException => false
    }


      val stab = s"taq1 = db.loadTable($table);schema(taq1).colDefs"
      val tblen = conn.run(stab)
//      val size = tblen.getColumn(0).rows()
//      val nameDB = new Array[String](size)
//      val typeDB = new Array[String](size)

      //      util.HashMap
      //      new immutable.HashMap()

//      for (i <- 0 until size) {
//        nameDB(i) = tblen.getColumn(0).get(i).getString
//        typeDB(i) = tblen.getColumn(1).get(i).getString
//      }
//      val tuples = nameDB.zip(typeDB)
//      println(nameDB)
//      println(typeDB)
//
//      println("==============")


      val sqldata = s"select bid , time from taq1 where date=2007.08.10, symbol='IBM'"
      val tabledata = conn.run(sqldata).asInstanceOf[BasicTable]
      println("columns : " + tabledata.columns())
      println("rows : " + tabledata.rows())

      println(tabledata)

      val nameDS = tabledata.getColumnName(1)

      println(" nameDS  " + nameDS)
      val dataType = tabledata.getDataType
      println("  dataType   " + dataType)
      val timedata = tabledata.getColumn("time")
      println("timedata" + timedata)

      val DataForm = tabledata.getDataForm
      println("DataForm" + tabledata.getDataForm)

      val DataCategory = tabledata.getDataCategory
      println("DataCategory" + tabledata.getDataCategory)

      val column1 = tabledata.getColumn(0)

      println(" column1 " + column1.get(1))
      println("  column1    " + column1)
      println("===================")
      val iterator = Iterator(tabledata)
      while (iterator.hasNext) {
        println(iterator.next())
      }

      val sqldata1 = s"select count(*) from taq1 where date=2007.08.10, symbol='IBM'"
      val tabledata1 = conn.run(sqldata1).asInstanceOf[BasicTable]
      println("  tabledata1   " + tabledata1)

      println("  tabledata1  val " + tabledata1.getColumn(0).get(0))
    }

  }




}
