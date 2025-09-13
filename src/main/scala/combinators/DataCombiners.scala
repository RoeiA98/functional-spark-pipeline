package combinators

/**
 * Custom combinators for functional composition
 * Advanced functional programming technique #1: Custom Combinator
 */
object DataCombiners {

  /**
   * Custom combinator that lifts a function to work with Options
   * This is a custom implementation of the Option functor
   */
  def liftOption[A, B](f: A => B): Option[A] => Option[B] = {
    case Some(value) => Some(f(value))
    case None => None
  }

  /**
   * Custom combinator for chaining validations
   * Combines multiple validation functions
   */
  def chainValidations[A](validations: List[A => Boolean]): A => Boolean = { value =>
    validations.forall(validation => validation(value))
  }

  /**
   * Custom combinator for parallel processing of functions
   * Takes multiple functions and applies them to the same input
   */
  def parallel[A, B](functions: List[A => B]): A => List[B] = { input =>
    functions.map(f => f(input))
  }

  /**
   * Custom combinator for conditional function application
   * Applies function only if condition is met
   */
  def conditional[A](condition: A => Boolean)(f: A => A): A => A = { input =>
    if (condition(input)) f(input) else input
  }

  /**
   * Combinator for function composition with error handling
   * Advanced functional programming technique #2: Functional Error Handling
   */
  def safeCompose[A, B, C](f: A => B, g: B => C): A => Either[String, C] = { input =>
    try {
      val intermediate = f(input)
      Right(g(intermediate))
    } catch {
      case ex: Exception => Left(s"Error in composition: ${ex.getMessage}")
    }
  }
}