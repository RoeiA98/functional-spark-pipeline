package main

import org.apache.spark.sql.SparkSession
import spark.{SparkOperations, SparkOperationsTrait}
import scala.util.{Success, Failure}

/**
 * Main application object for functional movie analytics pipeline.
 * Orchestrates data loading, analysis, and result saving operations.
 * Demonstrates functional error handling, pattern matching with case classes,
 * and comprehensive resource management for Spark applications.
 */
object Main {

  /**
   * Main entry point for the movie analytics application.
   * Creates SparkSession, loads data, performs analysis, and saves results.
   * Uses functional error handling with Try monads and pattern matching.
   *
   * @param args command line arguments (not used in this implementation)
   */
  def main(args: Array[String]): Unit = {
    // Input validation - method starts with validating arguments
    require(args != null, "Arguments array cannot be null")

    /*
     * Create SparkSession with optimized configuration
     * Uses adaptive query execution for better performance
     */
    val spark = SparkSession.builder()
      .appName("Functional Movie Analytics Pipeline")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    println("=== STARTING TO LOAD DATA ===")

    try {
      /*
       * Initialize Spark operations using trait for dependency abstraction
       * Demonstrates interface-based programming as recommended in style guide
       */
      val sparkOps: SparkOperationsTrait = new SparkOperations(spark)

      /*
       * Load data using functional composition with for-comprehension
       * Demonstrates functional error handling with Try monads
       */
      val loadResult = for {
        movies <- sparkOps.loadMovies("src/main/scala/data/movies.csv")
        ratings <- sparkOps.loadRatings("src/main/scala/data/ratings.csv")
      } yield (movies, ratings)

      /*
       * Pattern matching with case classes for functional result handling
       * Demonstrates functional error handling and resource management
       */
      loadResult match {
        case Success((movies, ratings)) => // Pattern matching for successful loading
          println("=== DATA LOADED SUCCESSFULLY ===")
          println(s"Movies loaded count: ${movies.count()}")
          println(s"Ratings loaded count: ${ratings.count()}")

          /*
           * Perform comprehensive analysis on loaded datasets
           * Method call with proper error propagation handling
           */
          sparkOps.performEnhancedAnalysis(movies, ratings)

          /*
           * Save results with functional error handling
           * Pattern matching for result validation and user feedback
           */
          sparkOps.saveResults(movies, ratings) match {
            case Success(_) => // Pattern matching for successful save operation
              println("\n✅ Results saved successfully!")
            case Failure(ex) => // Pattern matching for failed save operation
              println(s"\n❌ Error saving results: ${ex.getMessage}")
          }

        case Failure(exception) => // Pattern matching for data loading failure
          println(s"❌ Failed to load data: ${exception.getMessage}")
      }

    } finally {
      /*
       * Ensure proper resource cleanup regardless of execution outcome
       * Critical for Spark applications to prevent resource leaks
       */
      spark.stop()
    }
  }
}