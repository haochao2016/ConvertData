package test.connector

import com.xxdb.DBConnection
import com.xxdb.data.BasicTable

class testDolphinDBJARRE {

  def readerDB(conn : DBConnection, dbPath : String,table : String ) : Unit = {



    val existDB = s"existsDatabase($dbPath)"
    if (conn.run(existDB) == null) {
      throw new Exception(s"There'is no database named ${dbPath}")
    }
    else {
      val dben = conn.run(s"db=database($dbPath)")
      val stab = s"taq1 = db.loadTable($table);schema(taq1)"
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
