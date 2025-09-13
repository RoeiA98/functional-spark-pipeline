import Models.{Movie, Rating}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import transformations.DataLoader

/**
 * Unit tests demonstrating separation of pure logic from I/O
 * Tests functional programming components independently
 */
class TransformationsTest extends AnyFunSuite with Matchers {
  test("parseMovieLine should correctly parse a valid movie line with genres") {
    val line = "1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy"
    val expected = Some(Movie(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"))
    DataLoader.parseMovieLine(line) shouldEqual expected
  }

  test("parseMovieLine should correctly parse a valid movie line without genres") {
    val line = "2,Jumanji (1995),"
    val expected = Some(Movie(2, "Jumanji (1995)", ""))
    DataLoader.parseMovieLine(line) shouldEqual expected
  }

  test("parseMovieLine should return None for an invalid movie line") {
    val line = "Invalid Line"
    DataLoader.parseMovieLine(line) shouldEqual None
  }

  test("parseMovieLine should handle quotes in the title") {
    val line = "3,\"Grumpier Old Men (1995)\",Comedy|Romance"
    val expected = Some(Movie(3, "Grumpier Old Men (1995)", "Comedy|Romance"))
    DataLoader.parseMovieLine(line) shouldEqual expected
  }

  test("parseMovieLine should handle missing genres gracefully") {
    val line = "4,Waiting to Exhale (1995)"
    val expected = Some(Movie(4, "Waiting to Exhale (1995)", ""))
    DataLoader.parseMovieLine(line) shouldEqual expected
  }
}