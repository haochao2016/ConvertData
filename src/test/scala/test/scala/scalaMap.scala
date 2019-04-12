package test.scala

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object scalaMap {


  def main(args: Array[String]): Unit = {

    val map = new mutable.HashMap[String, ArrayBuffer[String]]()
    map.put("q1", new ArrayBuffer[String]())
//    map.put("q2", new ArrayBuffer[String]())

    for (i <- 0 until 1000 ) {
      val value = map.get("q1").get
      value += ("w" + i)
    }

    println(map.get("q1").get.length)

  }

}
