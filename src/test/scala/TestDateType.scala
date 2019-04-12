

import java.sql.{Date, Timestamp}

object TestDateType {

  def main(args: Array[String]): Unit = {

//    StructField("data", DateType)
//   println(DateType("2018-12-04"))
//    DataTypes.DateType("2018-12-04")
    println(Date.valueOf("2018-12-04"))
//    TimestampType
    println(Timestamp.valueOf("2018-12-04 03:21:56"))



  }

}
