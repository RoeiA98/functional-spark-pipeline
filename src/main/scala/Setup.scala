import org.apache.spark.sql.SparkSession

object Setup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Setup")
      .master("local[*]")
      .getOrCreate()

    println("Spark version: " + spark.version)
    println("Scala version: " + scala.util.Properties.versionString)
    println("Setup successful!")

    spark.stop()
  }
}