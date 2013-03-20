package arterial_research.socc.gamma
import breeze.stats.distributions.Gamma
import org.apache.commons.math.util.MathUtils

object GammaFunctions {

  def logLikelihood(gs:GammaState, x:Double):Double = {
    (new Gamma(gs.k, gs.theta)).logPdf(x)
  }
  
  def volumeStandardSimplex(n:Int):Double = {
    return 1.0 / MathUtils.factorialDouble(n)
  }
}