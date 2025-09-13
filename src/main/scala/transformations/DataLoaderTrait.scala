package transformations

import Models._

trait DataLoaderTrait {
  def parseMovieLine(line: String): Option[Movie]
}