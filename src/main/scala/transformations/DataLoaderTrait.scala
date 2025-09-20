package transformations

import Models._

/**
 * Trait defining data loading operations for movie analytics platform.
 * Provides contract for parsing CSV data into case class objects.
 * Demonstrates functional error handling with Option types and pattern matching.
 */
trait DataLoaderTrait {
  /**
   * Parse a single line from movies CSV file into Movie case class.
   * Uses pattern matching with case classes and functional error handling.
   *
   * @param line CSV line containing movie data, must not be null
   * @return Option containing Movie object if parsing succeeds, None otherwise
   */
  def parseMovieLine(line: String): Option[Movie]
}