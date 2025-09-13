package transformations

import data.{Movie, Rating, Tag}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}

/**
 * Pure functional data loading operations
 * Separates I/O from pure transformations
 */
object DataLoader {

  /**
   * Pure function to parse a movie CSV line
   * @param line CSV line as string
   * @return Option[Movie] - None if parsing fails
   */
  def parseMovieLine(line: String): Option[Movie] = {
    val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    if (parts.length >= 3) {
      Try {
        val movieId = parts(0).toInt
        val title = parts(1).replaceAll("\"", "")
        val genres = if (parts.length > 2) parts(2) else ""
        Movie(movieId, title, genres)
      }.toOption
    } else None
  }

  /**
   * Pure function to parse a rating CSV line
   * @param line CSV line as string
   * @return Option[Rating] - None if parsing fails
   */
  def parseRatingLine(line: String): Option[Rating] = {
    val parts = line.split(",")
    if (parts.length == 4) {
      Try {
        Rating(
          userId = parts(0).toInt,
          movieId = parts(1).toInt,
          rating = parts(2).toDouble,
          timestamp = parts(3).toLong
        )
      }.toOption
    } else None
  }

  /**
   * Pure function to parse a tag CSV line
   * @param line CSV line as string
   * @return Option[Tag] - None if parsing fails
   */
  def parseTagLine(line: String): Option[Tag] = {
    val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    if (parts.length >= 4) {
      Try {
        Tag(
          userId = parts(0).toInt,
          movieId = parts(1).toInt,
          tag = parts(2).replaceAll("\"", ""),
          timestamp = parts(3).toLong
        )
      }.toOption
    } else None
  }

  /**
   * Curried function for filtering ratings by minimum rating
   * Example of currying - returns a function that can be reused
   */
  val filterByMinRating: Double => Rating => Boolean = minRating => rating => rating.rating >= minRating

  /**
   * Higher-order function that takes a predicate and returns filtered ratings
   * Example of higher-order functions
   */
  def filterRatings(predicate: Rating => Boolean): List[Rating] => List[Rating] =
    ratings => ratings.filter(predicate)

  /**
   * Function composition example
   * Combines filtering and mapping operations
   */
  val processHighRatings: List[Rating] => List[Double] =
    filterRatings(filterByMinRating(4.0)) andThen (_.map(_.rating))

  /**
   * Advanced functional programming technique #3: Tail-recursive function
   * Calculates factorial using tail recursion with accumulator
   */
  @annotation.tailrec
  def calculateFactorial(n: Int, accumulator: Long = 1): Long = {
    if (n <= 1) accumulator
    else calculateFactorial(n - 1, n * accumulator)
  }

  /**
   * Tail-recursive function to find top N ratings from a list
   * More practical example for our domain
   */
  @annotation.tailrec
  private def findTopNRatings(ratings: List[Rating], n: Int, acc: List[Rating] = List.empty): List[Rating] = {
    if (n <= 0 || ratings.isEmpty) acc.reverse
    else {
      val maxRating = ratings.maxBy(_.rating)
      val remaining = ratings.filterNot(_ == maxRating)
      findTopNRatings(remaining, n - 1, maxRating :: acc)
    }
  }
}