package travel_time

/**
 * Indicates that the distribution provides a cumulative distribution.
 */
trait HasCumulative[T] {

  /**
   * The cumulative distribution.
   */
  def cdf(x: T): Double

}