package test.scala

import java.util

import com.xxdb.data.{BasicBooleanVector, BasicIntVector, BasicLongVector, BasicStringVector, Vector}

import scala.collection.mutable.ArrayBuffer

object scalaDolpType {

  def main(args: Array[String]): Unit = {
    val buffer = new ArrayBuffer[Any]()

    buffer += new ArrayBuffer[String]()
    buffer += new ArrayBuffer[Int]()
    buffer += new ArrayBuffer[Long]()
//    buffer += new ArrayBuffer[Boolean]()

    buffer(0).asInstanceOf[ArrayBuffer[String]] += "s1"
    buffer(0).asInstanceOf[ArrayBuffer[String]] += "s2"
    buffer(0).asInstanceOf[ArrayBuffer[String]] += "s3"



//    buffer.toArray
//    val a1 = new BasicStringVector(buffer(0).asInstanceOf[ArrayBuffer[String]].toArray)
//    val a2 = new BasicIntVector(buffer(1).asInstanceOf[ArrayBuffer[Int]].toArray)
//    val a3 = new BasicLongVector(buffer(2).asInstanceOf[ArrayBuffer[Long]].toArray)
////    val s4 = new BasicBooleanVector(buffer(3).asInstanceOf[Array[Boolean]])
//    val vectors = new util.ArrayList[Vector]()
//    vectors.add(a1)
//    vectors.add(a2)
//    vectors.add(a3)
//    for (bu <- buffer) {
//      for (b <- bu) {
//        print(b)
//      }
//    }

    val aa = buffer(0).asInstanceOf[ArrayBuffer[String]]
    for(a <- aa){
      println(a)
    }
    println("=============================")
    println(aa(0))
    println(aa(1))
    println(aa(2))



  }

}
