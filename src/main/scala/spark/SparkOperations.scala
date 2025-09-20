package spark

import Models._
import exceptions.MovieAnalyticsPlatformException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import transformations.DataLoader

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.expressions.Window

/**
 * Spark operations implementation for movie data processing.
 * Demonstrates functional transformations with Spark DataFrame API including
 * closures in transformations, pattern matching with case classes, and functional error handling.
 * This class provides comprehensive movie analytics functionality including
 * data loading, transformation, and analysis operations.
 *
 * @param spark SparkSession instance for all Spark operations
 */
class SparkOperations(spark: SparkSession) extends SparkOperationsTrait {
  import spark.implicits._

  /**
   * Load movies from CSV file using RDD API with functional error handling.
   * Uses RDD transformations to parse CSV data into Movie objects.
   * Demonstrates closures in Spark transformations and functional error handling.
   *
   * @param filePath path to the movies.csv file, must not be null or empty
   * @return Try containing Dataset of Movie objects
   * @throws MovieAnalyticsPlatformException if file loading or parsing fails
   */
  override def loadMovies(filePath: String): Try[Dataset[Movie]] = {
    // Input validation - method starts with validating arguments
    if (filePath == null || filePath.trim.isEmpty) {
      return Failure(new MovieAnalyticsPlatformException("File path cannot be null or empty"))
    }

    Try {
      /*
       * Load movies using RDD API for demonstration of functional programming
       * Filter header line and parse each data line to Movie object
       * Uses closures in filter and flatMap transformations
       */
      val moviesRDD: RDD[Movie] = spark.sparkContext
        .textFile(filePath)
        .filter(!_.startsWith("movieId")) // Closure capturing startsWith method
        .flatMap(DataLoader.parseMovieLine) // Closure calling external function with pattern matching

      moviesRDD.toDS()
    }.recoverWith {
      case ex: Exception =>
        Failure(new MovieAnalyticsPlatformException("Failed to load movies from file", ex))
    }
  }

  /**
   * Load ratings from CSV file using Dataset API.
   * Uses Spark SQL functions for type-safe data loading with functional error handling.
   *
   * @param filePath path to the ratings.csv file, must not be null or empty
   * @return Try containing Dataset of Rating objects
   * @throws MovieAnalyticsPlatformException if file loading fails
   */
  override def loadRatings(filePath: String): Try[Dataset[Rating]] = {
    // Input validation - method starts with validating arguments
    if (filePath == null || filePath.trim.isEmpty) {
      return Failure(new MovieAnalyticsPlatformException("File path cannot be null or empty"))
    }

    Try {
      /*
       * Load ratings using Dataset API with automatic schema inference
       * Cast columns to appropriate types for type safety
       * Demonstrates functional error handling with Try wrapper
       */
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
        .as[Rating] // Pattern matching with case class conversion
    }.recoverWith {
      case ex: Exception =>
        Failure(new MovieAnalyticsPlatformException("Failed to load ratings from file", ex))
    }
  }

  /**
   * Calculate average ratings and movie counts by genre.
   * Demonstrates functional transformations: join, filter, explode, groupBy, aggregation.
   * Uses pattern matching with case classes for type-safe operations.
   *
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @return Dataset containing genre statistics
   * @throws MovieAnalyticsPlatformException if calculation fails
   */
  private def calculateGenreStats(movies: Dataset[Movie], ratings: Dataset[Rating]): Dataset[GenreStats] = {
    // Input validation - method starts with validating arguments
    require(movies != null, "Movies dataset cannot be null")
    require(ratings != null, "Ratings dataset cannot be null")

    /*
     * Join movies with ratings to combine movie metadata with rating data
     * This creates the foundation for genre-based analysis
     */
    val movieRatings = movies.join(ratings, "movieId")

    /*
     * Split pipe-separated genres and explode into individual rows
     * Filter out movies without genre information using closures
     */
    val genreRatings = movieRatings
      .filter(col("genres") =!= "(no genres listed)") // Closure in filter transformation
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .select("genre", "rating")

    /*
     * Group by genre and calculate aggregated statistics
     * Round average rating to 2 decimal places for readability
     * Pattern matching with GenreStats case class for type safety
     */
    genreRatings
      .groupBy("genre")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("movieCount")
      )
      .as[GenreStats] // Pattern matching with case class
  }

  /**
   * Find top-rated movies with minimum rating threshold.
   * Uses aggregation to filter out movies with insufficient ratings.
   * Demonstrates functional transformations and pattern matching.
   *
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @param minRatings minimum number of ratings required, must be positive
   * @return Dataset containing movie title, average rating, and rating count
   * @throws MovieAnalyticsPlatformException if analysis fails
   */
  private def findTopRatedMovies(movies: Dataset[Movie], ratings: Dataset[Rating], minRatings: Int = 100): Dataset[(String, Double, Long)] = {
    // Input validation - method starts with validating arguments
    require(movies != null, "Movies dataset cannot be null")
    require(ratings != null, "Ratings dataset cannot be null")
    require(minRatings > 0, "Minimum ratings must be positive")

    /*
     * Calculate movie statistics by aggregating ratings per movie
     * Filter movies with insufficient rating count for statistical significance
     * Uses closures in filter and groupBy transformations
     */
    val movieStats = ratings
      .groupBy("movieId")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= minRatings) // Closure in filter

    /*
     * Join with movie titles and select relevant columns
     * Pattern matching with tuple case class for type-safe return
     */
    movies
      .join(movieStats, "movieId")
      .select(
        col("title"),
        col("avgRating"),
        col("ratingCount")
      )
      .as[(String, Double, Long)] // Pattern matching with tuple
  }

  /**
   * Perform comprehensive movie analytics with advanced statistics.
   * Generates multiple analytical reports including temporal and genre-based analysis.
   * Demonstrates window functions, pattern matching, and complex transformations.
   *
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @throws MovieAnalyticsPlatformException if analysis fails
   */
  override def performEnhancedAnalysis(movies: Dataset[Movie], ratings: Dataset[Rating]): Unit = {
    // Input validation - method starts with validating arguments
    require(movies != null, "Movies dataset cannot be null")
    require(ratings != null, "Ratings dataset cannot be null")

    println("\n=== ENHANCED MOVIE ANALYTICS ===")

    /*
     * Analysis 1: Top movie genre for each year with its rating
     * Demonstrates regex extraction, window functions, and complex aggregations
     */
    println("\n1. Top Movie Genre by Year:")
    val topGenreByYear = movies
      .filter(col("genres") =!= "(no genres listed)") // Closure in filter
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1990) // Closure with compound condition
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .join(ratings, "movieId")
      .groupBy("year", "genre")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("movieCount")
      )
      .withColumn("rank", row_number().over(
        Window.partitionBy("year").orderBy(desc("movieCount"), desc("avgRating"))
      ))
      .filter(col("rank") === 1) // Closure in final filter
      .select(
        col("year").as("Year"),
        col("genre").as("Genre"),
        col("avgRating").as("Rating")
      )
      .orderBy("year")

    topGenreByYear.show(35, truncate = false)

    /*
     * Analysis 2: Top rated movie for each decade
     * Demonstrates mathematical transformations and window ranking
     */
    println("\n2. Top Rated Movie by Decade:")
    val topMovieByDecade = movies
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1900) // Closure with null check
      .withColumn("decade", ((col("year") / 10).cast("int") * 10))
      .join(ratings, "movieId")
      .groupBy("decade", "movieId", "title")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= 50) // Closure for minimum rating threshold
      .withColumn("rank", row_number().over(
        Window.partitionBy("decade").orderBy(desc("avgRating"), desc("ratingCount"))
      ))
      .filter(col("rank") === 1) // Closure for top rank selection
      .select(
        col("decade").as("Decade"),
        col("title").as("Title"),
        col("avgRating").as("Rating")
      )
      .orderBy("decade")

    topMovieByDecade.show(20, truncate = false)

    /*
     * Analysis 3: Top rated movie for each genre in each decade
     * Demonstrates complex nested grouping and window operations
     */
    println("\n3. Top Rated Movie for each Genre in each Decade:")
    val topMovieByGenreDecade = movies
      .filter(col("genres") =!= "(no genres listed)") // Closure filtering invalid genres
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1980) // Closure with date filter
      .withColumn("decade", (col("year") / 10).cast("int") * 10)
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .join(ratings, "movieId")
      .groupBy("decade", "genre", "movieId", "title")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= 20) // Closure for rating count threshold
      .withColumn("rank", row_number().over(
        Window.partitionBy("decade", "genre").orderBy(desc("avgRating"), desc("ratingCount"))
      ))
      .filter(col("rank") === 1) // Closure for rank selection
      .select(
        col("decade").as("Decade"),
        col("genre").as("Genre"),
        col("title").as("Title"),
        col("avgRating").as("Rating")
      )
      .orderBy("decade", "genre")

    /*
     * Get distinct decades and create separate tables for better readability
     * Demonstrates functional collection operations and pattern matching
     */
    val decades = topMovieByGenreDecade.select("decade").distinct().collect().map(_.getAs[Int]("decade")).sorted

    // Display results by decade using functional iteration
    decades.foreach { decade => // Closure in foreach iteration
      println(s"\n${decade}s Top Movies by Genre:")
      topMovieByGenreDecade
        .filter(col("decade") === decade) // Closure with decade comparison
        .select(
          col("Genre"),
          col("Title"),
          col("Rating")
        )
        .orderBy("genre")
        .show(50, truncate = false)
    }
  }

  /**
   * Save analysis results to text files.
   * Creates results directory and writes genre statistics and top movies.
   * Demonstrates functional error handling with resource management.
   *
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @return Try indicating success or failure of save operation
   * @throws MovieAnalyticsPlatformException if saving fails
   */
  override def saveResults(movies: Dataset[Movie], ratings: Dataset[Rating]): Try[Unit] = {
    // Input validation - method starts with validating arguments
    require(movies != null, "Movies dataset cannot be null")
    require(ratings != null, "Ratings dataset cannot be null")

    Try {
      import java.io.{File, PrintWriter}

      /*
       * Calculate results to save using previously defined methods
       * Demonstrates functional composition and method chaining
       */
      val genreStats = calculateGenreStats(movies, ratings).collect()
      val topMovies = findTopRatedMovies(movies, ratings, 100).collect()

      // Create results directory if it doesn't exist
      val resultsDir = new File("results")
      if (!resultsDir.exists()) resultsDir.mkdirs()

      /*
       * Write genre statistics to file with proper resource management
       * Pattern matching with case class destructuring in foreach
       */
      val genreWriter = new PrintWriter(new File("results/genre_analysis.txt"))
      try {
        genreWriter.println("Genre,AvgRating,MovieCount")
        genreStats.foreach { stats => // Closure with case class pattern matching
          genreWriter.println(s"${stats.genre},${stats.avgRating},${stats.movieCount}")
        }
      } finally {
        genreWriter.close() // Resource cleanup
      }

      /*
       * Write top movies to file with tuple pattern matching
       * Demonstrates destructuring of tuple case classes
       */
      val movieWriter = new PrintWriter(new File("results/top_movies.txt"))
      try {
        movieWriter.println("Title,AvgRating,RatingCount")
        topMovies.foreach { case (title, rating, count) => // Pattern matching with tuple destructuring
          movieWriter.println(s"$title,$rating,$count")
        }
      } finally {
        movieWriter.close() // Resource cleanup
      }

      println("Results saved to results/genre_analysis.txt and results/top_movies.txt")
    }.recoverWith {
      case ex: Exception => // Pattern matching in exception handling
        Failure(new MovieAnalyticsPlatformException("Failed to save results", ex))
    }
  }
}