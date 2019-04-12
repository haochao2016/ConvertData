import org.apache.spark.sql.types._

object TestSchema {
  def main(args: Array[String]): Unit = {

    val a = StructType(StructField("id", StringType) ::
      StructField("name", StringType) ::
      StructField("age", IntegerType) ::
      Nil)

    println(a(1).name)
    println(a(2).name)
    println(a(1).dataType)
    println(a(1).metadata)
    println(a(2).dataType)

    val type1 : DataType = a(1).dataType
    val type2 : DataType = a(2).dataType
    println("==============================")
    type1 match {
      case StringType => println("StringType")
      case IntegerType => println("IntegerType")
      case _ => println("NONON")
    }
    println("==============================")
    type2 match {
      case StringType => println("StringType")
      case IntegerType => println("IntegerType")
      case _ => println("NONON")
    }

  }

}
