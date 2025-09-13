package spark

import Models._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.Try

trait SparkOperationsTrait {
  def loadMovies(filePath: String): Try[Dataset[Movie]]
  def loadRatings(filePath: String): Try[Dataset[Rating]]
  def performEnhancedAnalysis(movies: Dataset[Movie], ratings: Dataset[Rating]): Unit
  def saveResults(movies: Dataset[Movie], ratings: Dataset[Rating]): Try[Unit]
}