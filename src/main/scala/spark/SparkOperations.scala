package spark

import data.{Movie, Rating, Tag, GenreStats}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import transformations.DataLoader
import scala.util.{Try, Success, Failure}

/**
 * Spark operations for movie data processing
 * Demonstrates functional transformations with Spark
 */
class SparkOperations(spark: SparkSession) {
  import spark.implicits._

  /**
   * Load movies using RDD API with functional error handling
   * @param filePath path to movies.csv
   * @return Try[Dataset[Movie]] - functional error handling
   */
  def loadMovies(filePath: String): Try[Dataset[Movie]] = Try {
    val moviesRDD: RDD[Movie] = spark.sparkContext
      .textFile(filePath)
      .filter(!_.startsWith("movieId")) // Remove header
      .flatMap(DataLoader.parseMovieLine) // flatMap - Spark operation 1

    moviesRDD.toDS()
  }

  /**
   * Load ratings using Dataset API
   * @param filePath path to ratings.csv
   * @return Try[Dataset[Rating]]
   */
  def loadRatings(filePath: String): Try[Dataset[Rating]] = Try {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .select(
        col("userId").cast("int"),
        col("movieId").cast("int"),
        col("rating").cast("double"),
        col("timestamp").cast("long")
      )
      .as[Rating]
  }

  /**
   * Calculate average ratings by genre using multiple Spark operations
   * Demonstrates: map, filter, groupBy, aggregation
   */
  def calculateGenreStats(movies: Dataset[Movie], ratings: Dataset[Rating]): Dataset[GenreStats] = {
    // Join movies with ratings - Spark operation 2 (join)
    val movieRatings = movies.join(ratings, "movieId")

    // Split genres and explode - functional transformation
    val genreRatings = movieRatings
      .filter(col("genres") =!= "(no genres listed)") // filter - Spark operation 3
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .select("genre", "rating")

    // Group by genre and calculate stats - Spark operation 4 (groupBy)
    genreRatings
      .groupBy("genre")
      .agg(
        avg("rating").as("avgRating"),
        count("*").as("movieCount")
      )
      .as[GenreStats]
  }

  /**
   * Find top-rated movies using functional operations
   * Demonstrates reduceByKey pattern
   */
  def findTopRatedMovies(movies: Dataset[Movie], ratings: Dataset[Rating], minRatings: Int = 100): Dataset[(String, Double, Long)] = {
    val movieStats = ratings
      .groupBy("movieId")
      .agg(
        avg("rating").as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= minRatings)

    movies
      .join(movieStats, "movieId")
      .select(
        col("title").as("_1"),
        col("avgRating").as("_2"),
        col("ratingCount").as("_3")
      )
      .orderBy(desc("_2"))
      .as[(String, Double, Long)]
  }

  /**
   * Closure example - captures external variable
   * Demonstrates closures in Spark transformations
   */
  def filterMoviesByDecade(movies: Dataset[Movie], decade: Int): Dataset[Movie] = {
    // Closure captures 'decade' variable
    val decadeFilter = (title: String) => {
      val yearPattern = "\\((\\d{4})\\)".r
      yearPattern.findFirstMatchIn(title) match {
        case Some(m) =>
          val year = m.group(1).toInt
          year >= decade && year < decade + 10
        case None => false
      }
    }

    // Register UDF that uses closure
    val decadeFilterUDF = udf(decadeFilter)
    movies.filter(decadeFilterUDF(col("title")))
  }

  /**
   * Comprehensive movie analytics using all Spark operations
   */
  def performComprehensiveAnalysis(movies: Dataset[Movie], ratings: Dataset[Rating]): Unit = {
    import spark.implicits._

    println("\n=== COMPREHENSIVE MOVIE ANALYTICS ===")

    // 1. Genre Statistics
    println("\n1. Top 10 Genres by Average Rating:")
    val genreStats = calculateGenreStats(movies, ratings)
    genreStats.orderBy(desc("avgRating")).show(10)

    // 2. Top Movies
    println("\n2. Top 10 Highest Rated Movies (min 1000 ratings):")
    findTopRatedMovies(movies, ratings, 1000).show(10)

    // 3. Movies by Decade Analysis
    println("\n3. Movies by Decade (2000s):")
    val movies2000s = filterMoviesByDecade(movies, 2000)
    println(s"Movies from 2000s: ${movies2000s.count()}")
    movies2000s.show(5)

    // 4. User Rating Patterns
    println("\n4. Rating Distribution:")
    ratings.groupBy("rating")
      .count()
      .orderBy("rating")
      .show()

    // 5. Most Active Users
    println("\n5. Most Active Users (Top 10):")
    ratings.groupBy("userId")
      .count()
      .orderBy(desc("count"))
      .show(10)
  }

  /**
   * Save results to files - required by project specs
   */
  /**
   * Save results to simple text files
   */
  def saveResults(movies: Dataset[Movie], ratings: Dataset[Rating]): Try[Unit] = Try {
    import java.io.{File, PrintWriter}

    val genreStats = calculateGenreStats(movies, ratings).collect()
    val topMovies = findTopRatedMovies(movies, ratings, 100).collect()

    // Create results directory
    val resultsDir = new File("results")
    if (!resultsDir.exists()) resultsDir.mkdirs()

    // Write genre stats
    val genreWriter = new PrintWriter(new File("results/genre_analysis.txt"))
    genreWriter.println("Genre,AvgRating,MovieCount")
    genreStats.foreach { stats =>
      genreWriter.println(s"${stats.genre},${stats.avgRating},${stats.movieCount}")
    }
    genreWriter.close()

    // Write top movies
    val movieWriter = new PrintWriter(new File("results/top_movies.txt"))
    movieWriter.println("Title,AvgRating,RatingCount")
    topMovies.foreach { case (title, rating, count) =>
      movieWriter.println(s"$title,$rating,$count")
    }
    movieWriter.close()

    println("Results saved to results/genre_analysis.txt and results/top_movies.txt")
  }
}