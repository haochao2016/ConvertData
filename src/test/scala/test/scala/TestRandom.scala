package test.scala

import scala.collection.mutable

object TestRandom {

  def main(args: Array[String]): Unit = {

    val set =  Set(1,32,345,45,434,678,9,65,3442).toArray

    var str = scala.util.Random.nextInt(set.size)
    println(str)
    println(set(str))

//    val s2s = new mutable.HashMap[String, String]()
//    s2s.put("q1","q1")
//    s2s.put("q2","q2")
//    s2s.put("q3","q3")
//    s2s.put("q4","q4")
//    s2s.put("q5","q5")
////    s2s.get(s2s.take())
//
//    val sd = s2s.take(2)
//    println(sd)
  }

}
