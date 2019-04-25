package test.scala

import scala.collection.mutable.ArrayBuffer

object ArrayBuffer1 {

  def main(args: Array[String]): Unit = {

    val sdf = ArrayBuffer[String]("32453","sdfsa")
    println(sdf(0))
    println(sdf(1))

    println("9" > "11")

    val s : Int = 3434
    val l : Long = 234

    val sd = vv("12","1")
    val df = vv("2","2")
  }

  def vv(s :String, q: String) : Any = q match {
    case "1" => s.toInt
    case "2" => s.toLong

  }

}
