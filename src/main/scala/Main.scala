import org.apache.spark.sql.SparkSession
import spark.SparkOperations
import transformations.{DataLoader, MovieAnalytics}
import org.apache.log4j.{Level, Logger}
import scala.util.{Success, Failure}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Functional Movie Analytics Pipeline")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    println("=== STARTING TO LOAD DATA ===")
    try {
      val sparkOps = new SparkOperations(spark)

      // Functional composition for data loading with error handling
      val loadResult = for {
        movies <- sparkOps.loadMovies("src/main/scala/data/movies.csv")
        ratings <- sparkOps.loadRatings("src/main/scala/data/ratings.csv")
      } yield (movies, ratings)

      loadResult match {
        case Success((movies, ratings)) =>
          println("=== DATA LOADED SUCCESSFULLY ===")
          println(s"Movies loaded count: ${movies.count()}")
          println(s"Ratings loaded count: ${ratings.count()}")

          // Comprehensive Spark-based analytics
          sparkOps.performEnhancedAnalysis(movies, ratings)

          // Save results using functional error handling
          sparkOps.saveResults(movies, ratings) match {
            case Success(_) => println("\n✅ Results saved successfully!")
            case Failure(ex) => println(s"\n❌ Error saving results: ${ex.getMessage}")
          }

        case Failure(exception) =>
          println(s"❌ Failed to load data: ${exception.getMessage}")
      }

    } finally {
      spark.stop()
    }
  }
}