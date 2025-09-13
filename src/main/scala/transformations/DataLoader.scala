package transformations

import Models._
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
}