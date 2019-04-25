package test.dolphindb

import com.xxdb.DBConnection
import com.xxdb.data.{BasicAnyVector, BasicDateVector, BasicString, BasicTable, Vector}

object dolphindbchemafeature {

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

    val stab = s"${table} = database('${dbPath}').loadTable('${table}');schema(${table}).partitionColumnName"
    val tblen = conn.run(stab).asInstanceOf[BasicString]

    println(tblen)

    val vector = conn.run(s"schema(${table}).partitionSchema").asInstanceOf[Vector]
    val table1 = conn.run(s"schema(${table}).colDefs").asInstanceOf[BasicTable]


    val vect = vector.asInstanceOf[BasicAnyVector]
    val entity = vect.getEntity(1).asInstanceOf[Vector]
    entity.get(1)


    println(vect.rows())

    for (i <- 0 until(table1.rows())) {
      println(table1.getColumn(0).get(i).getString +"   "+ table1.getColumn(1).get(i).getString)

    }



    conn.close()


  }

}
