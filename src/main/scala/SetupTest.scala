import org.apache.spark.sql.SparkSession

object SetupTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Setup Test")
      .master("local[*]")
      .getOrCreate()

    println("Spark version: " + spark.version)
    println("Scala version: " + scala.util.Properties.versionString)
    println("Setup successful!")

    spark.stop()
  }
}