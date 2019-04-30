package test.scala

object Array2 {

  def main(args: Array[String]): Unit = {
    val numList1 = List(1,2,3,4,5);
    val numList2 = List(11,2,12,13);

    val vd = Array("2007-05-15","2007-05-16","2007-05-17","2007-05-18","2007-05-19","2007-05-20")

     val str = vd.map(v => v.replace("-", ".")).mkString(",")
    println(str)


      import scala.util.control.Breaks._
//    import scala.util.control.BreakControl

//  breakable {
//    numList1.foreach(n1 => numList2.foreach(n2 => if (n2 == n1) {
//      //       return
//        break
//    } else {
//
//      println(n1 + "    " + n2)
//    }
//
//    ))
//
//  }

    breakable {for( f <- numList1){
      for(b <- numList2){
        if(f == b){
          break;
        } else {
          System.out.println(f + "    "+ b);
        }


      }
      }
    }


  }

}
