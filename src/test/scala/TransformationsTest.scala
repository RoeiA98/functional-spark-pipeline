import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import transformations.{DataLoader, MovieAnalytics}
import data.{Movie, Rating}

/**
 * Unit tests demonstrating separation of pure logic from I/O
 * Tests functional programming components independently
 */
class TransformationsTest extends AnyFunSuite with Matchers {

  test("parseMovieLine should parse valid CSV line") {
    val csvLine = "1,\"Toy Story (1995)\",Adventure|Animation|Children|Comedy|Fantasy"
    val result = DataLoader.parseMovieLine(csvLine)

    result shouldBe Some(Movie(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"))
  }

  test("parseMovieLine should handle invalid input") {
    val invalidLine = "invalid,csv"
    val result = DataLoader.parseMovieLine(invalidLine)

    result shouldBe None
  }

  test("parseRatingLine should parse valid rating") {
    val csvLine = "1,1193,5.0,978300760"
    val result = DataLoader.parseRatingLine(csvLine)

    result shouldBe Some(Rating(1, 1193, 5.0, 978300760L))
  }

  test("categorizeRating should classify ratings correctly") {
    val excellentRating = Rating(1, 1, 4.5, 123456L)
    val goodRating = Rating(1, 1, 3.5, 123456L)
    val poorRating = Rating(1, 1, 1.5, 123456L)

    MovieAnalytics.categorizeRating(excellentRating) shouldBe "Excellent"
    MovieAnalytics.categorizeRating(goodRating) shouldBe "Good"
    MovieAnalytics.categorizeRating(poorRating) shouldBe "Poor"
  }

  test("extractYear should extract year from title") {
    val movieWithYear = Movie(1, "Toy Story (1995)", "Animation")
    val movieWithoutYear = Movie(2, "Unknown Movie", "Drama")

    MovieAnalytics.extractYear(movieWithYear.title) shouldBe Some(1995)
    MovieAnalytics.extractYear(movieWithoutYear.title) shouldBe None
  }

  test("filterByMinRating curried function should work") {
    val ratings = List(
      Rating(1, 1, 4.5, 123L),
      Rating(2, 2, 3.0, 123L),
      Rating(3, 3, 2.0, 123L)
    )

    val highRatingsFilter = DataLoader.filterByMinRating(4.0)
    val filteredRatings = DataLoader.filterRatings(highRatingsFilter)(ratings)

    filteredRatings should have length 1
    filteredRatings.head.rating shouldBe 4.5
  }

  test("tail recursive factorial should calculate correctly") {
    DataLoader.calculateFactorial(5) shouldBe 120
    DataLoader.calculateFactorial(0) shouldBe 1
    DataLoader.calculateFactorial(1) shouldBe 1
  }

  test("safeCalculateAverage should handle empty list") {
    val result = MovieAnalytics.safeCalculateAverage(List.empty)
    result.isLeft shouldBe true
  }

  test("safeCalculateAverage should calculate average correctly") {
    val result = MovieAnalytics.safeCalculateAverage(List(1.0, 2.0, 3.0, 4.0, 5.0))
    result shouldBe Right(3.0)
  }

  test("custom combinator liftOption should work correctly") {
    import combinators.DataCombiners.liftOption

    val addOne = (x: Int) => x + 1
    val liftedAddOne = liftOption(addOne)

    liftedAddOne(Some(5)) shouldBe Some(6)
    liftedAddOne(None) shouldBe None
  }

  test("chainValidations combinator should combine validations") {
    import combinators.DataCombiners.chainValidations

    val isPositive = (x: Int) => x > 0
    val isEven = (x: Int) => x % 2 == 0
    val combinedValidation = chainValidations(List(isPositive, isEven))

    combinedValidation(4) shouldBe true
    combinedValidation(-2) shouldBe false
    combinedValidation(3) shouldBe false
  }

  test("processRatingsSafely should handle monadic composition") {
    val validRatings = List(Rating(1, 1, 4.0, 123L), Rating(2, 2, 5.0, 123L))
    val emptyRatings = List.empty[Rating]

    val validResult = MovieAnalytics.processRatingsSafely(validRatings)
    val emptyResult = MovieAnalytics.processRatingsSafely(emptyRatings)

    validResult.isRight shouldBe true
    emptyResult.isLeft shouldBe true
  }
}