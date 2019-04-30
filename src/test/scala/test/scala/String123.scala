package test.scala

object String123 {


  def main(args: Array[String]): Unit = {

    val str1 = "AB"
    val str2 = "ACZ"

    println(str1 > "ABCSD")
    println(str1 < "ABCSD")
    println(str2 > "ABCSD")
    println(str2 < "ABCSD")

    val str3 = "12345%6"
    println(str3.substring(1, str3.lastIndexOf("%")))


  }

}
