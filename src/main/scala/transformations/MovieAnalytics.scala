package transformations

import Models._
import scala.util.{Try, Success, Failure}

/**
 * Advanced movie analytics using functional programming
 * Demonstrates pattern matching, function composition, and advanced FP techniques
 */
object MovieAnalytics {

  /**
   * Advanced functional programming technique #4: Pattern matching with case classes
   * Analyzes rating patterns and categorizes them
   */
  def categorizeRating(rating: Rating): String = rating match {
    case Rating(_, _, r, _) if r >= 4.5 => "Excellent"
    case Rating(_, _, r, _) if r >= 3.5 => "Good"
    case Rating(_, _, r, _) if r >= 2.5 => "Average"
    case Rating(_, _, r, _) if r >= 1.5 => "Poor"
    case _ => "Terrible"
  }

  /**
   * Extract year from movie title using pattern matching
   */
  def extractYear(title: String): Option[Int] = {
    val yearPattern = """.*\((\d{4})\).*""".r
    title match {
      case yearPattern(year) => Try(year.toInt).toOption
      case _ => None
    }
  }

  /**
   * Advanced functional programming technique #5: Functional error handling with Either
   */
  def safeCalculateAverage(ratings: List[Double]): Either[String, Double] = {
    if (ratings.isEmpty) {
      Left("Cannot calculate average of empty list")
    } else {
      Try(ratings.sum / ratings.length) match {
        case Success(avg) => Right(avg)
        case Failure(ex) => Left(s"Error calculating average: ${ex.getMessage}")
      }
    }
  }

  /**
   * Monadic composition using for-comprehension
   */
  def processRatingsSafely(ratings: List[Rating]): Either[String, (Double, Int, String)] = {
    for {
      ratingValues <- Right(ratings.map(_.rating))
      average <- safeCalculateAverage(ratingValues)
      count = ratings.length
      category = if (average >= 4.0) "High Quality"
      else if (average >= 3.0) "Good Quality"
      else "Low Quality"
    } yield (average, count, category)
  }
}