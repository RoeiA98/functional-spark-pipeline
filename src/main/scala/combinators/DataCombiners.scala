package combinators

/**
 * Custom combinators for functional composition
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
}