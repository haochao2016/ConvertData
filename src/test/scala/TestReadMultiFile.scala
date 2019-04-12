import org.apache.spark.sql.SparkSession

object TestReadMultiFile {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("")
      .master("local")
      .getOrCreate()

//    spark.read.csv().union()

  }

}
