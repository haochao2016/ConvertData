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
    val table = """"tab1""""
    //    val table = """"sparktb1""""


    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)

//    val dben = conn.run(s"db=database($dbPath)")

      val stab = s"tab= database($dbPath).loadTable(${table});schema(tab).colDefs"
      val tblen = conn.run(stab).asInstanceOf[BasicTable]
      //      val tblen = conn.run(stab).asInstanceOf[BasicDictionary]
      val partiCol = conn.run("schema(tab).partitionColumnName").isInstanceOf[com.xxdb.data.Void]


      val size = tblen.getColumn(0).rows()
      val nameDB = new Array[String](size)
      val typeDB = new Array[String](size)
//
      for (i <- 0 until size) {
        nameDB(i) = tblen.getColumn(0).get(i).getString
        typeDB(i) = tblen.getColumn(1).get(i).getString
      }
      val tuples = nameDB.zip(typeDB)
      println(nameDB)
      println(typeDB)

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
