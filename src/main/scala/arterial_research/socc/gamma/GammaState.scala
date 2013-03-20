package arterial_research.socc.gamma

import breeze.stats.distributions.{ Gamma => GammaNLP }
import org.apache.commons.math.special.Gamma

//import arterial_research.socc.gamma.GammaState;


/**
 * Description of the state of gamma distribution.
 *
 * Follows wikipedia convention for the parameters.
 */
case class GammaState(
    val k: Double,
    val theta: Double) {
  
  def isApprox(other: GammaState)(precision: Double): Boolean = {
    (math.abs(k - other.k) / k < precision) &&
      (math.abs(theta - other.theta) / theta < precision)
  }

  override def toString = "GammaState[k=%f, theta=%f]" format (k, theta)
  
  def scaled(x:Double) = GammaState(k, theta * x)
  
  def mean = k * theta

  def variance = k * theta * theta
  
  def sufficientStatistics:GammaNLP.SufficientStatistic = {
    val mol = Gamma.digamma(k) + math.log(theta)
    GammaNLP.SufficientStatistic(1.0, mol, mean)
  }
}

object GammaState {
  def fromMeanVariance(mean:Double, variance:Double):GammaState = {
    val k = (mean * mean) / variance
    val theta = variance / mean
    GammaState(k, theta)
  }
}
