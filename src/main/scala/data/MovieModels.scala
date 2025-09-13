package data

/**
 * Case class representing a movie
 * @param movieId unique movie identifier
 * @param title movie title with year
 * @param genres pipe-separated list of genres
 */
case class Movie(movieId: Int, title: String, genres: String)

/**
 * Case class representing a rating
 * @param userId unique user identifier
 * @param movieId unique movie identifier
 * @param rating user rating (0.5-5.0)
 * @param timestamp rating timestamp
 */
case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

/**
 * Case class representing a tag
 * @param userId unique user identifier
 * @param movieId unique movie identifier
 * @param tag user-generated tag
 * @param timestamp tag timestamp
 */
case class Tag(userId: Int, movieId: Int, tag: String, timestamp: Long)

/**
 * Case class for analyzed results
 * @param genre movie genre
 * @param avgRating average rating for the genre
 * @param movieCount number of movies in genre
 */
case class GenreStats(genre: String, avgRating: Double, movieCount: Long)