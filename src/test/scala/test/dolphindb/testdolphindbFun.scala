package test.dolphindb

import com.xxdb.DBConnection
import com.xxdb.data.{BasicString, BasicStringVector, BasicTable}
import test.dolphindb.dolphindbchemafeature._

object testdolphindbFun {

  val ip = "115.239.209.224"
  val port = 16961
  val user = "admin"
  val passwd = "123456"
  //  val dbPath = "dfs://chao/Spark/db"
  //  val table = "sparktb1"
  val dbPath = "dfs://chao/TAQ/db"
  val table = "taq"


  def main(args: Array[String]): Unit = {

    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)

    val stab = //s"${table} = database('${dbPath}').loadTable('${table}');" +
                      s" ctl = getControllerAlias();" +
                                    s"exec site from rpc(ctl,getClusterPerf) where mode = 0"

    val tblen = conn.run(stab).asInstanceOf[BasicStringVector]
//    for (i <- 0 until(tblen.rows()); j <- 0 until(tblen.columns())) {
//      println(tblen.getColumn(j).get(i))
//    }

    for (s <- 0 until(tblen.rows())) {
//      println(tblen.get(s))
      val ips = tblen.get(s).toString
      print(ips)
      print("     ")
      print(ips.substring(0, ips.indexOf(":") ))
      print("     ")
      println(ips.substring(ips.indexOf(":")+1 , ips.lastIndexOf(":")))
    }

    conn.close()



  }

}
