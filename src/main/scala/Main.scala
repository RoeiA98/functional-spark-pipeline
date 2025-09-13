import org.apache.spark.sql.SparkSession
import spark.SparkOperations
import transformations.{DataLoader, MovieAnalytics}
import org.apache.log4j.{Level, Logger}
import scala.util.{Success, Failure}

/**
 * Functional Data Processing Pipeline with Apache Spark
 *
 * This application demonstrates advanced functional programming techniques
 * using Scala and Apache Spark to analyze movie rating data.
 *
 * ==Functional Programming Techniques Demonstrated:==
 *  - Pure functions and immutable data structures
 *  - Higher-order functions and currying
 *  - Pattern matching with case classes
 *  - Tail recursion
 *  - Custom combinators
 *  - Functional error handling with Try/Either
 *  - Function composition
 *
 * ==Spark Operations Used:==
 *  - flatMap: Data parsing and flattening
 *  - filter: Data filtering by conditions
 *  - join: Combining movies with ratings
 *  - groupBy: Aggregating data by genre/user
 *  - map: Data transformations
 *  - aggregations: avg, count operations
 *
 * @author [Your Name]
 * @version 1.0
 */
object Main {

  /**
   * Main entry point for the functional movie analytics pipeline
   *
   * Processes MovieLens dataset using functional programming principles
   * and generates comprehensive analytics reports.
   *
   * @param args Command line arguments (not used)
   */
  def main(args: Array[String]): Unit = {
    // Suppress verbose Spark logging for cleaner output
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("Functional Movie Analytics Pipeline")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

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
          println(s"Movies: ${movies.count()}")
          println(s"Ratings: ${ratings.count()}")

          // Demonstrate functional programming techniques
          println("\n=== FUNCTIONAL PROGRAMMING DEMONSTRATIONS ===")

          // Tail recursion demonstration
          println(s"Factorial of 5 (tail recursive): ${DataLoader.calculateFactorial(5)}")

          // Pattern matching and function composition
          val sampleMovies = movies.take(3)
          sampleMovies.foreach { movie =>
            val (title, year, category) = MovieAnalytics.processMovieData(movie)
            println(s"Movie: $title, Year: ${year.getOrElse("Unknown")}, Category: $category")
          }

          // Comprehensive Spark-based analytics
          sparkOps.performComprehensiveAnalysis(movies, ratings)

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