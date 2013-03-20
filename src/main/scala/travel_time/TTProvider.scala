package travel_time

import netconfig.Link
import netconfig.Route
import org.joda.time.DateTime
import breeze.stats.distributions.ContinuousDistr
import breeze.stats.distributions.Moments
import netconfig.NetconfigException

/**
 * Travel time provider
 */
trait TTProvider[L <: Link] { // Cannot make it covariant due to the route.

//  type TTDistribution = ContinuousDistr[Double] with HasCumulative[Double]

  def travelTime(start_time: DateTime, route: Route[L]): ScalaTravelTimeDistribution
}


object TTProvider {

  def wrap[L <: Link](ttp: TTProvider[L]): ScalaTTProvider[L] = ttp match {
    case sttp: ScalaTTProvider[_] => sttp.asInstanceOf[ScalaTTProvider[L]]
    case _ => new TTProviderWrapper(ttp)
  }

  private[this] class TTProviderWrapper[L <: Link](ttp: TTProvider[L]) extends ScalaTTProvider[L] {
    def travelTimeSafe(start_time: DateTime, route: Route[L]): Either[ScalaTravelTimeDistribution, NetconfigException] = {
      try {
        Left(ttp.travelTime(start_time, route))
      } catch {
        case e: NetconfigException => Right(e)
        case e => throw e; Right(null)
      }
    }
  }
}

