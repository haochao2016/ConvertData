package test.scala

object Array2 {

  def main(args: Array[String]): Unit = {
    val numList1 = List(1,2,3,4,5);
    val numList2 = List(11,2,12,13);
      import scala.util.control.Breaks._


  breakable {
    numList1.foreach(n1 => numList2.foreach(n2 => if (n2 == n1) {
      //       return
        break
    } else {

      println(n1 + "    " + n2)
    }

    ))

  }


  }

}
