package spark

import Models._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import transformations.DataLoader
import scala.util.Try
import org.apache.spark.sql.expressions.Window

/**
 * Spark operations for movie data processing
 * Demonstrates functional transformations with Spark
 */
class SparkOperations(spark: SparkSession) extends SparkOperationsTrait {
  import spark.implicits._

  /**
   * Load movies using RDD API with functional error handling
   * @param filePath path to movies.csv
   */
  def loadMovies(filePath: String): Try[Dataset[Movie]] = Try {
    val moviesRDD: RDD[Movie] = spark.sparkContext
      .textFile(filePath)
      .filter(!_.startsWith("movieId"))
      .flatMap(DataLoader.parseMovieLine)

    moviesRDD.toDS()
  }

  /**
   * Load ratings using Dataset API
   * @param filePath path to ratings.csv
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
  private def calculateGenreStats(movies: Dataset[Movie], ratings: Dataset[Rating]): Dataset[GenreStats] = {
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
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("movieCount")
      )
      .as[GenreStats]
  }

  /**
   * Find top-rated movies using functional operations
   * Demonstrates reduceByKey pattern
   */
  private def findTopRatedMovies(movies: Dataset[Movie], ratings: Dataset[Rating], minRatings: Int = 100): Dataset[(String, Double, Long)] = {
    val movieStats = ratings
      .groupBy("movieId")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= minRatings)

    movies
      .join(movieStats, "movieId")
      .select(
        col("title"),
        col("avgRating"),
        col("ratingCount")
      )
      .as[(String, Double, Long)]
  }

  /**
   * Enhanced movie analytics with more interesting statistics
   */
  def performEnhancedAnalysis(movies: Dataset[Movie], ratings: Dataset[Rating]): Unit = {

    println("\n=== ENHANCED MOVIE ANALYTICS ===")

    // 1. Top movie genre for each year with its rating
    println("\n1. Top Movie Genre by Year:")
    val topGenreByYear = movies
      .filter(col("genres") =!= "(no genres listed)")
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1990)
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
      .filter(col("rank") === 1)
      .select("year", "genre", "avgRating")
      .orderBy("year")

    topGenreByYear.show(35, truncate = false)

    // 2. Top rated movie for each decade
    println("\n2. Top Rated Movie by Decade:")
    val topMovieByDecade = movies
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1900)
      .withColumn("decade", ((col("year") / 10).cast("int") * 10))
      .join(ratings, "movieId")
      .groupBy("decade", "movieId", "title")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= 50)
      .withColumn("rank", row_number().over(
        Window.partitionBy("decade").orderBy(desc("avgRating"), desc("ratingCount"))
      ))
      .filter(col("rank") === 1)
      .select("decade", "title", "avgRating")
      .orderBy("decade")

    topMovieByDecade.show(20, truncate = false)

    // 3. Top rated movie for each genre in each decade
    println("\n3. Top Rated Movie for each Genre in each Decade:")
    val topMovieByGenreDecade = movies
      .filter(col("genres") =!= "(no genres listed)")
      .withColumn("year", regexp_extract(col("title"), "\\((\\d{4})\\)", 1).cast("int"))
      .filter(col("year").isNotNull && col("year") >= 1980)
      .withColumn("decade", (col("year") / 10).cast("int") * 10)
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .join(ratings, "movieId")
      .groupBy("decade", "genre", "movieId", "title")
      .agg(
        round(avg("rating"), 2).as("avgRating"),
        count("*").as("ratingCount")
      )
      .filter(col("ratingCount") >= 20)
      .withColumn("rank", row_number().over(
        Window.partitionBy("decade", "genre").orderBy(desc("avgRating"), desc("ratingCount"))
      ))
      .filter(col("rank") === 1)
      .select("decade", "genre", "title", "avgRating")
      .orderBy("decade", "genre")

    // Get distinct decades and create separate tables
    val decades = topMovieByGenreDecade.select("decade").distinct().collect().map(_.getAs[Int]("decade")).sorted

    decades.foreach { decade =>
      println(s"\n${decade}s Top Movies by Genre:")
      topMovieByGenreDecade
        .filter(col("decade") === decade)
        .select("genre", "title", "avgRating")
        .orderBy("genre")
        .show(50, truncate = false)
    }
  }

  /*
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