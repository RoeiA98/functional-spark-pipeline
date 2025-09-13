package transformations

import data.{Movie, Rating, GenreStats}
import combinators.DataCombiners._
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
   * Pattern matching for movie genres
   */
  def categorizeGenre(movie: Movie): String = movie match {
    case Movie(_, _, genres) if genres.contains("Action") && genres.contains("Adventure") => "Action-Adventure"
    case Movie(_, _, genres) if genres.contains("Comedy") && genres.contains("Romance") => "Romantic Comedy"
    case Movie(_, _, genres) if genres.contains("Sci-Fi") => "Science Fiction"
    case Movie(_, _, genres) if genres.contains("Horror") => "Horror"
    case Movie(_, _, genres) if genres.contains("Drama") => "Drama"
    case Movie(_, _, genres) if genres.contains("Animation") => "Animation"
    case _ => "Other"
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
   * Function composition example - combine multiple transformations
   */
  val processMovieData: Movie => (String, Option[Int], String) = { movie =>
    val category = categorizeGenre(movie)
    val year = extractYear(movie.title)
    val titleClean = movie.title.replaceAll("""\(\d{4}\)""", "").trim
    (titleClean, year, category)
  }

  /**
   * Higher-order function for rating analysis
   */
  def analyzeRatings[T](ratings: List[Rating],
                        transform: Rating => T,
                        predicate: T => Boolean): List[T] = {
    ratings.map(transform).filter(predicate)
  }

  /**
   * Curried function for filtering by decade
   */
  val filterByDecade: Int => Movie => Boolean = decade => movie => {
    extractYear(movie.title) match {
      case Some(year) => year >= decade && year < decade + 10
      case None => false
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