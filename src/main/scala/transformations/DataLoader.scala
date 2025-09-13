package transformations

import Models._
import scala.util.Try

object DataLoader extends DataLoaderTrait {

  private def safeParse[T](line: String)(parseLogic: String => T): Option[T] = {
    Try(parseLogic(line)).toOption
  }

  override def parseMovieLine(line: String): Option[Movie] = {
    safeParse(line) { line =>
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
      parts match {
        case Array(movieId, title, genres, _*) =>
          Movie(movieId.toInt, title.replaceAll("\"", ""), genres)
        case Array(movieId, title) =>
          Movie(movieId.toInt, title.replaceAll("\"", ""), "")
        case _ =>
          throw new IllegalArgumentException("Invalid CSV format")
      }
    }
  }
}