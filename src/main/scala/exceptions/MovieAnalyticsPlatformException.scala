package exceptions

/**
 * Project-specific exception for movie analytics platform
 * @param message error message describing the malfunction
 * @param cause root cause of the exception
 */
class MovieAnalyticsPlatformException(message: String, cause: Throwable = null)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}