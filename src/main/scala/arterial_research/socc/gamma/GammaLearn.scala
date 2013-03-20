package arterial_research.socc.gamma

import breeze.stats.distributions.{ Gamma => GammaNLP }
import org.apache.commons.math.special.Gamma
import breeze.linalg._
import breeze.linalg.DenseVector._
import core_extensions.MMLogging


object GammaLearn extends MMLogging {
  import org.apache.commons.math.special.Gamma._

  def mle(vals:Array[Double], weights:Array[Double], prior:GammaState, prior_strength:Double): GammaState = {
    val vals_ = DenseVector(vals)
    val log_vals = DenseVector(vals).map(math.log _)
    val weights_ = DenseVector(weights)
    // The prior sufficient statistics
    val mean_prior = prior.mean
    // See wikipedia for this formula
    val meanOfLogs_prior = Gamma.digamma(prior.k) + math.log(prior.theta)
    // Checking some basic properties:
    assert(math.log(mean_prior) >= meanOfLogs_prior)
    //    logInfo("meanOfLogs_prior = %f" format (meanOfLogs_prior))
    // Sum of all the weights given to the observations
    val n = weights.sum + prior_strength
    assert(n>0)
    // Weighted sum, includes the prior.
    val meanOfLogs = (log_vals.dot(weights_) + prior_strength * meanOfLogs_prior) / n
    val mean = (vals_.dot(weights_) + prior_strength * mean_prior) / n
    //    logInfo("Starting learning for link: n=%f, mlog=%f, m=%f, obj=%f" format (n, meanOfLogs, mean, math.log(mean)-meanOfLogs))
    assert(math.log(mean) >= meanOfLogs)
    val ss = GammaNLP.SufficientStatistic(n, meanOfLogs, mean)
    GammaLearn.mle(ss)
  }
  
  def sufficientStats(vals: Array[Double], weights: Array[Double]):GammaNLP.SufficientStatistic = {
      val n = weights.sum
      // No observation?
      if (n == 0) {
        GammaNLP.SufficientStatistic(0.0, 0.0, 0.0)
      } else {
        val vals_ = DenseVector(vals)
        val log_vals = DenseVector(vals).map(math.log _)
        val weights_ = DenseVector(weights)
        val meanOfLogs = log_vals.dot(weights_) / n
        val mean = vals_.dot(weights_) / n
        GammaNLP.SufficientStatistic(n, meanOfLogs, mean)
      }
  }
  
  def mle2(vals: Array[Double], weights: Array[Double], prior: GammaState, prior_strength: Double): GammaState = {
    require(weights.forall(_ >= 0))
    require(prior_strength >= 0)
    // The prior sufficient statistics
    val prior_ss = prior.sufficientStatistics * prior_strength
    val obs_ss = sufficientStats(vals,weights)
//    logInfo("Prior SS: "+prior_ss)
//    logInfo("Observation SS: "+obs_ss)
    val ss = prior_ss + obs_ss
//    logInfo("Learning SS: "+obs_ss)
    GammaLearn.mle(ss)
  }
  
  
  def mle(ss: GammaNLP.SufficientStatistic):GammaState = {
    val s = math.log(ss.mean) - ss.meanOfLogs
    assert(s > 0, s) // check concavity
    val k_approx = approx_k(s)
    assert(k_approx > 0, k_approx)
    val k = Nwt_Rph_iter_for_k(k_approx, s)
    val theta = ss.mean / (k)
    GammaState(k, theta)
  }
  /*
   * s = log( x_hat) - log_x_hat
   */
  def approx_k(s: Double): Double = {
    // correct within 1.5%
    (3 - s + math.sqrt(math.pow((s - 3), 2) + 24 * s)) / (12 * s)
  }

  def Nwt_Rph_iter_for_k(k: Double, s: Double): Double = {
    /*
     * For a more precise estimate, use Newton-Raphson updates
     */
    val k_new = k - (math.log(k) - digamma(k) - s) / (1.0 / k - trigamma(k))
    if (math.abs(k - k_new) / math.abs(k_new) < 1.0e-4)
      k_new
    else
      Nwt_Rph_iter_for_k(k_new, s)
  }

}