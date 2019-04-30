import com.xxdb.DBConnection
import com.xxdb.data.{BasicAnyVector, BasicIntVector, BasicStringVector, BasicTable, Vector}

import scala.collection.mutable.ArrayBuffer

object QueryDolphinDBSchema {

  def main(args: Array[String]): Unit = {
    val ip = "115.239.209.224"
    val port = 16961
    val user = "admin"
    val passwd = "123456"
    val dbPath = """"/home/chao/apps/DolphinDB/dolpPath/compo/valuelist2""""
//        val dbPath = """"dfs://chao/TAQ/db""""
    val table = """"c2""""
//        val table = """"taq""""


    val conn = new DBConnection()
    val loginVal = conn.connect(ip, port, user, passwd)

//    val dben = conn.run(s"db=database($dbPath)")

      val stab = s"tab= database($dbPath).loadTable(${table});schema(tab).colDefs"
      val tblen = conn.run(stab).asInstanceOf[BasicTable]
      //      val tblen = conn.run(stab).asInstanceOf[BasicDictionary]
//      val partiCol = conn.run("schema(tab).partitionColumnName").asInstanceOf[BasicStringVector]
      val partitionColumnName = conn.run("schema(tab).partitionColumnName")//.asInstanceOf[BasicIntVector]
      val partiType = conn.run("schema(tab).partitionType")//.asInstanceOf[BasicIntVector]
      val partitionSchemao = conn.run(s"schema(tab).partitionSchema")
      val partitionSchema = partitionSchemao.asInstanceOf[BasicAnyVector]

    for (i <- 0 until(partitionSchema.rows())) {
//      if (partitionSchema.get(i).isInstanceOf[BasicAnyVector]) {
//
//      } else {
//
//      }

      val vector = partitionSchema.getEntity(i).asInstanceOf[Vector]
      val strings = new ArrayBuffer[String]()
      for (j <- 0 until(vector.rows())) {
        println(partitionSchema.getEntity(i).getClass)
        if (partitionSchema.getEntity(i).isInstanceOf[BasicAnyVector]) {
          val vector1 = partitionSchema.getEntity(i).asInstanceOf[BasicAnyVector]
          for (k <- 0 until(vector1.rows())) {
            println(vector1.getEntity(k).getClass)
            if (vector1.getEntity(k).isInstanceOf[Vector]) {
              val vector2 = vector1.getEntity(k).asInstanceOf[Vector]
              for (v <- 0 until(vector2.rows())) {
                strings += vector2.get(v).toString
              }
            } else {
              strings += vector1.get(k).toString
            }

          }
        } else {
          println(vector.getElementClass)
          strings += vector.get(j).toString
        }
      }


    }





    println(partiType)


      val size = tblen.getColumn(0).rows()
      val nameDB = new Array[String](size)
      val typeDB = new Array[String](size)
      for (i <- 0 until size) {
        nameDB(i) = tblen.getColumn(0).get(i).getString
        typeDB(i) = tblen.getColumn(1).get(i).getString
      }
      val tuples = nameDB.zip(typeDB)
      println(nameDB)
      println(typeDB)

//      partitionSchema.rows()
//      for (i <- 0  until(partitionSchema.rows())) {
//        val vc = partitionSchema.getEntity(i).asInstanceOf[Vector]
//        for (j <- 0 until(vc.rows())){
//          println(vc.get(j).toString)
//        }
//      }


        val sites = conn.run(s"ctl = getControllerAlias();" +
          s"  exec site from rpc(ctl,getClusterPerf) where mode = 0").asInstanceOf[BasicStringVector]
        for (s <- 0 until(sites.rows())) {
          println(sites.get(s).toString)

        }
  }

}
