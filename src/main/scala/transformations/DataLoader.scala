package transformations

import Models._
import scala.util.Try

/**
 * Object implementing data loading operations for movie analytics platform.
 * Provides CSV parsing functionality with functional error handling.
 * Demonstrates closures in transformations, pattern matching with case classes,
 * and functional error handling using Try and Option types.
 */
object DataLoader extends DataLoaderTrait {

  /**
   * Safe parsing wrapper that handles exceptions functionally.
   * Uses Try monad for functional error handling and converts to Option.
   *
   * @param line input string to parse, must not be null
   * @param parseLogic parsing function that may throw exceptions
   * @tparam T return type of successful parsing
   * @return Option containing parsed result or None if parsing fails
   */
  private def safeParse[T](line: String)(parseLogic: String => T): Option[T] = {
    // Input validation - method starts with validating arguments
    if (line == null) return None

    /*
     * Functional error handling using Try monad
     * Converts exceptions to None, successful parsing to Some
     */
    Try(parseLogic(line)).toOption // Closure with functional error handling
  }

  /**
   * Parse a single line from movies CSV file into Movie case class.
   * Handles quoted fields and variable number of columns.
   * Uses pattern matching with case classes for type-safe parsing.
   *
   * @param line CSV line containing movie data, must not be null
   * @return Option containing Movie object if parsing succeeds, None otherwise
   */
  override def parseMovieLine(line: String): Option[Movie] = {
    // Input validation - method starts with validating arguments
    if (line == null || line.trim.isEmpty) return None

    safeParse(line) { line => // Closure with parsing logic
      /*
       * Split CSV line handling quoted fields properly
       * Regex ensures commas inside quotes are not used as delimiters
       */
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

      /*
       * Pattern matching with case classes for different CSV formats
       * Handles movies with and without genre information
       */
      parts match {
        case Array(movieId, title, genres, _*) => // Pattern matching with variable arguments
          Movie(
            movieId.toInt,
            title.replaceAll("\"", ""), // Remove quotes from title
            genres
          )
        case Array(movieId, title) => // Pattern matching for minimal format
          Movie(
            movieId.toInt,
            title.replaceAll("\"", ""),
            ""
          )
        case _ => // Pattern matching catch-all
          throw new IllegalArgumentException(s"Invalid CSV format for line: $line")
      }
    }
  }
}