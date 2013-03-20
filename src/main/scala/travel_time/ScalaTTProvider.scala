package travel_time

import netconfig.Link
import netconfig.Route
import org.joda.time.DateTime
import breeze.stats.distributions.ContinuousDistr
import breeze.stats.distributions.Moments
import netconfig.NetconfigException

/**
 * Trait for scala users.
 */
trait ScalaTTProvider[L <: Link] extends TTProvider[L] {

  type SafeReturnType = Either[ScalaTravelTimeDistribution, NetconfigException]
  /**
   * Returns a travel time distribution.
   */
  def travelTimeSafe(start_time: DateTime, route: Route[L]): SafeReturnType

  def travelTime(start_time: DateTime, route: Route[L]): ScalaTravelTimeDistribution = {
    travelTimeSafe(start_time, route) match {
      case Left(tt) => tt
      case Right(e) => throw new NetconfigException(e, ""); null
    }
  }
}
