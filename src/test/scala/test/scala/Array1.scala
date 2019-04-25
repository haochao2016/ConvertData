package test.scala

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Array1 {

  def main(args: Array[String]): Unit = {

    val s = "20070825"
    val date = LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyyMMdd"))
    println(date)

    val strings = Array("1231")
    strings ++ Array("324")
    strings.foreach(println)


  }

}
