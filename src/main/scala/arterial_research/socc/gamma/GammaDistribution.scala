package arterial_research.socc.gamma
import breeze.stats.distributions.ContinuousDistr
import breeze.stats.distributions.Gamma
import org.apache.commons.math.special.{Gamma => AGamma}
import travel_time.HasCumulative

class GammaDistribution private (gs: GammaState)
  extends Gamma(gs.k, gs.theta)
  with HasCumulative[Double] {

  def cdf(x:Double) = {
    AGamma.regularizedGammaP(gs.k, x / gs.theta)
  }
}

object GammaDistribution {

  def apply(gs: GammaState) = new GammaDistribution(gs)
}