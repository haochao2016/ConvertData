import com.xxdb.DBConnection
import com.xxdb.data.{BasicStringVector, BasicTable, Vector}

object QueryDolphinDB {

  def main(args: Array[String]): Unit = {
    val ip = "115.239.209.224"
    val port = 16961
    val user = "admin"
    val passwd = "123456"
    val dbPath = """"/home/chao/apps/DolphinDB/dolpPath""""
    //    val dbPath = """"dfs://chao/Spark/db""""
    val table = """"tab""""
    //    val table = """"sparktb1""""


    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)

    val dben = conn.run(s"db=database($dbPath)")
      val stab = s"tab= database($dbPath).loadTable(${table});schema(tab).colDefs"
      val tblen = conn.run(stab).asInstanceOf[BasicTable]
      //      val tblen = conn.run(stab).asInstanceOf[BasicDictionary]
      val size = tblen.getColumn(0).rows()
      val nameDB = new Array[String](size)
      val typeDB = new Array[String](size)
//
//      val partitionSchema = conn.run(s"schema(taq1).partitionSchema").asInstanceOf[Vector]
//      //      println(stab1.get(0))
//      partitionSchema.rows()
//      println("===============23423423===============================")
//      for (i <- 0  until(partitionSchema.rows())) {
//        //        println(partitionSchema.get(i))
//      }
//
//      //      val sqldata11 = s"select bid , time from taq1 where date=${partitionSchema.get(30)}"
//      //      val sqltad = conn.run(sqldata11).asInstanceOf[BasicTable]
//
//      val dfas =  conn.run(s"schema(taq1).partitionColumnName").asInstanceOf[BasicStringVector]
//      println(dfas)
//      println("================2342342342==============================")
//
//
//
//      //      util.HashMap
//      //      new immutable.HashMap()
//
      for (i <- 0 until size) {
        nameDB(i) = tblen.getColumn(0).get(i).getString
        typeDB(i) = tblen.getColumn(1).get(i).getString
      }
      val tuples = nameDB.zip(typeDB)
      println(nameDB)
      println(typeDB)
//
//      println("==============")
//
//
      val sqldata = s"select * from tab"
      val tabledata = conn.run(sqldata).asInstanceOf[BasicTable]
      println( " all count :" + tabledata.rows())
      for  (i <- 0 until(tabledata.rows())) {
        for (j <- 0 until(tabledata.columns())){
          print(tabledata.getColumn(j).get(i))
          print("\t")
        }
        println
      }
  }

}
