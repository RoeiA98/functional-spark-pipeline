package spark

import Models._
import exceptions.MovieAnalyticsPlatformException
import org.apache.spark.sql.Dataset
import scala.util.Try

/**
 * Trait defining Spark operations interface for movie analytics platform.
 * Provides contract for data loading, analysis, and result saving operations.
 * All methods may throw MovieAnalyticsPlatformException for platform-specific errors.
 */
trait SparkOperationsTrait {

  /**
   * Load movies from CSV file using RDD API
   * @param filePath path to movies CSV file, must not be null or empty
   * @return Try containing Dataset of Movie objects
   * @throws MovieAnalyticsPlatformException if loading fails
   */
  def loadMovies(filePath: String): Try[Dataset[Movie]]

  /**
   * Load ratings from CSV file using Dataset API
   * @param filePath path to ratings CSV file, must not be null or empty
   * @return Try containing Dataset of Rating objects
   * @throws MovieAnalyticsPlatformException if loading fails
   */
  def loadRatings(filePath: String): Try[Dataset[Rating]]

  /**
   * Perform comprehensive analytics on movie and rating data
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @throws MovieAnalyticsPlatformException if analysis fails
   */
  def performEnhancedAnalysis(movies: Dataset[Movie], ratings: Dataset[Rating]): Unit

  /**
   * Save analysis results to files
   * @param movies Dataset of Movie objects, must not be null
   * @param ratings Dataset of Rating objects, must not be null
   * @return Try indicating success or failure
   * @throws MovieAnalyticsPlatformException if saving fails
   */
  def saveResults(movies: Dataset[Movie], ratings: Dataset[Rating]): Try[Unit]
}