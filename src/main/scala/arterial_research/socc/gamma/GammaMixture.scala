package arterial_research.socc.gamma

import breeze.stats.distributions.{ Gamma => GammaNLP, ContinuousDistr }
import breeze.linalg._
import common.Multinomial
import breeze.linalg.DenseVector._
import core_extensions.MMLogging
import org.apache.commons.math.special.Gamma
import travel_time.HasCumulative
import breeze.stats.distributions.Moments

case class GammaMixtureRepr(val states: Seq[GammaState], val logCoeffs: Seq[Double])

/**
 * Mixture of gamma distributions. Useful for computing the sum of independent
 * gamma distributions.
 */
class GammaMixture private (
  gss: Array[GammaState],
  normalized_log_coeffs: Array[Double]) extends ContinuousDistr[Double] with HasCumulative[Double] with Moments[Double] {

  private[this] lazy val coeffs = normalized_log_coeffs.map(math.exp _)
  private[this] val gamas = gss.map(GammaDistribution.apply _)
  private[this] lazy val uni_gen = new Multinomial(coeffs)

  assert(coeffs.forall(_ >= 0), coeffs.mkString(" "))
  assert(coeffs.sum >= 1 - 1e-3, (coeffs.mkString(" "), coeffs.sum))
  assert(coeffs.sum <= 1 + 1e-3, (coeffs.mkString(" "), coeffs.sum))

  override def logPdf(x: Double): Double = {
    val logpdfs = gamas.map(_.logPdf(x))
    for (idx <- 0 until gss.size) {
      logpdfs(idx) += normalized_log_coeffs(idx)
    }
    numerics.logSum(logpdfs)
  }

  def repr = GammaMixtureRepr(states, normalized_log_coeffs)

  def cdf(x: Double) = {
    var res = 0.0
    for (idx <- 0 until coeffs.size) {
      res += coeffs(idx) * gamas(idx).cdf(x)
    }
    res
  }

  def unnormalizedLogPdf(x: Double) = logPdf(x)

  def logNormalizer = 0.0

  def draw(): Double = {
    val idx = uni_gen.sample()
    gamas(idx).draw()
  }

  def coefficients: IndexedSeq[Double] = coeffs

  def states: IndexedSeq[GammaState] = gss

  def mode = Double.NaN

  def entropy = Double.NaN

  def mean = {
    var res = 0.0
    for (idx <- 0 until coeffs.size) {
      res += coeffs(idx) * gamas(idx).mean
    }
    res
  }

  def variance = {
    val m = mean
    var res = -m * m
    for (idx <- 0 until coeffs.size) {
      val m_i = gamas(idx).mean
      val v_i = gamas(idx).variance
      res += coeffs(idx) * (m_i * m_i + v_i)
    }
    res
  }
}

object GammaMixture extends MMLogging {

  def apply(states: Seq[GammaState], coeffs: Seq[Double]): GammaMixture = {
    val gss = states.toArray
    assert(gss.length == coeffs.length)
    assert(coeffs.forall(_ >= 0), coeffs.mkString(" "))
    assert(coeffs.sum >= 1 - 1e-3, (coeffs.mkString(" "), coeffs.sum))
    assert(coeffs.sum <= 1 + 1e-3, (coeffs.mkString(" "), coeffs.sum))
    new GammaMixture(gss, coeffs.map(math.log _).toArray)
  }

  def fromLogUnnormalizedCoeffs(states: Seq[GammaState], log_coeffs: Seq[Double]): GammaMixture = {
    val gss = states.toArray
    val lcoeffs = log_coeffs.toArray
    assert(gss.length == lcoeffs.length)
    val scale = numerics.logSum(lcoeffs)
    assert(!(scale.isNaN() || scale.isInfinite()))
    val ulcoeffs = lcoeffs.map(_ - scale)
    new GammaMixture(gss, ulcoeffs)
  }

  def positiveCombinationOfGammas(gammas: Array[GammaState], weights: Array[Double], K: Int = default): GammaMixture = {
    val scaled_gss = (gammas.zip(weights)).map(z => z._1.scaled(z._2))
    sumOfGamma(scaled_gss, K)
  }

  def sumOfGammaBreeze(gammas: Array[GammaState], K: Int = default): GammaMixture = {
    // See web.njit.edu/~abdi/gamma.pdf
    // Annoyingly enough, this paper does not follow the alpha/beta convention
    // from wikipedia to parametrize the distribution.
    // Sort the gamma distributions so that the first one has the smallest theta
    // This is an exact copy of the python reference code
    // One trivial case:
    assert(!gammas.isEmpty)
    if (gammas.size == 1) {
      return new GammaMixture(gammas, Array(1.0))
    }
    val alphas = DenseVector(gammas.map(_.k))
    val betas = DenseVector(gammas.map(_.theta))
    //    println("betas "+betas)
    val betas_inv = betas :^ (-1.0)
    //    println("betas_inv "+betas_inv)
    val beta1 = betas.min
    val deltas = DenseVector.zeros[Double](K)
    //  deltas[0] = 1.0
    deltas(0) = 1.0
    //  for k in range(1, K):
    for (k <- 1 until K) {
      //    for i in range(k+1):
      for (i <- 0 to k) {
        //  val z = alphas.dot((1 - beta1 / betas)**i)
        val z0 = (betas_inv * (-beta1) + 1.0) :^ (i.toDouble)
        val z = alphas.dot(z0)
        //  deltas[k] += deltas[k-i] * z / k
        deltas(k) += deltas(k - i) * z / k.toDouble
      }
    }
    //    println("deltas "+deltas)
    //  C = np.exp(np.log(beta1 / betas).dot(alphas))
    val z0log = (betas_inv * beta1).map(math.log _)
    logInfo("z0log: " + z0log)
    val C = math.exp(z0log.dot(alphas))
    logInfo("C " + C)
    //  alpha = np.sum(alphas)
    val alpha = alphas.sum
    //  ks = np.arange(K)
    val ks = DenseVector((0 until K).map(_.toDouble).toArray)
    //  k_coeffs = ks + alpha
    val k_coeffs = ks + alpha
    val theta = beta1
    //  thetas = beta1 * np.ones_like(k_coeffs)
    //  new_gammas = [GammaState(k, theta) for k,theta in zip(k_coeffs, thetas)]
    val new_gammas = k_coeffs.iterator.map(z => GammaState(z._2, theta)).toArray
    val coeffs = (deltas * C)
    if (math.abs(coeffs.sum - 1.0) > 1e-3) {
      logWarning("Potential lack of convergence for Gamma mixture:\n%s\n%s ".format(gammas.mkString(" "), coeffs.toString))
    }
    GammaMixture(new_gammas, (coeffs :/ coeffs.sum).data)
  }

  val default = 200
  val max_K = 300000

  def sumOfGamma(gammas: Array[GammaState], K: Int = default) = sumOfGammaAdaptive(gammas, K)

  def sumOfGammaAdaptive(gammas: Array[GammaState], K: Int = default, increase_ratio: Double = 1.5): GammaMixture = {
    val sg = sumOfGammaNoBreeze(gammas, K, true)
    if (K >= max_K) {
      logInfo("Maximum depth reached, returning non-optimal value")
      sumOfGammaNoBreeze(gammas, K, false)
    } else {
      if (sg == null) {
        val next_K = (K * increase_ratio).toInt
        //        logInfo("Retrying with K=%d..." format next_K)
        sumOfGammaAdaptive(gammas, next_K, increase_ratio)
      } else {
        sg
      }
    }
  }

  def sumOfGammaNoBreeze(gammas: Array[GammaState], K: Int = default, fail_on_lack_convergence: Boolean = false): GammaMixture = {
    // See web.njit.edu/~abdi/gamma.pdf
    // Annoyingly enough, this paper does not follow the alpha/beta convention
    // from wikipedia to parametrize the distribution.
    // Sort the gamma distributions so that the first one has the smallest theta
    // This is an exact copy of the python reference code
    // One trivial case:
    assert(!gammas.isEmpty)
    if (gammas.size == 1) {
      return new GammaMixture(gammas, Array(0.0))
    }
    val ks = gammas.map(_.k)
    val thetas = gammas.map(_.theta)
    val log_deltas = GammaMixture.logDeltaApproximate(ks, thetas, K)
    val log_C = {
      val x = GammaMixture.logC(ks, thetas)
      if (x.isNaN() || x.isInfinite()) {
        logWarning("Computing C failed for Gamma mixture: K = %d, logC = %f, gammas = %s".format(K, x, gammas.mkString(" ")))
        0.0
      } else {
        x
      }
    }
    val coeffs = log_deltas.map(d => math.exp(d + log_C))
    if (math.abs(coeffs.sum - 1.0) > 1e-2) {
      if (fail_on_lack_convergence) {
        return null
      } else {
        logWarning("Potential lack of convergence for Gamma mixture: K = %d, diff = %f, states = %s".format(K, coeffs.sum - 1.0, gammas.mkString(" ")))
      }
    }
    val k = ks.sum
    val theta = thetas.min
    val new_gammas = (0 until K).map(u => {
      val k_u = k + u
      GammaState(k_u, theta)
    }).toArray
    GammaMixture.fromLogUnnormalizedCoeffs(new_gammas, log_deltas)
  }

  def logC(ks: Array[Double], thetas: Array[Double]): Double = {
    val k = ks.sum
    var res = 0.0
    for (idx <- 0 until ks.size) {
      res += -ks(idx) * math.log(thetas(idx))
    }
    val theta = thetas.min
    res += k * math.log(theta)
    res
  }

  def logDelta(ks: Array[Double], thetas: Array[Double], num_values: Int): Array[Double] = {
    val theta = thetas.min
    val k = ks.sum
    val N = ks.size
    val log_rhos = thetas.map(theta_ => 1.0 - theta / theta_).map(math.log _)
    val log_ks = ks.map(math.log _)
    val log_thetas = ks.map(math.log _)

    val log_gammas = Array.fill(num_values)(Double.NaN)
    val log_deltas = Array.fill(num_values)(Double.NaN)

    // Temp buffers
    val buff = Array.fill(N)(Double.NaN)
    val buff2 = Array.fill(num_values)(Double.NegativeInfinity)

    // Compute log_gammas first
    for (u <- 0 until num_values) {
      for (idx <- 0 until N) {
        buff(idx) = log_ks(idx) + log_rhos(idx) * u
      }
      log_gammas(u) = numerics.logSum(buff)
    }

    // Compute the log deltas
    log_deltas(0) = 0.0
    for (u <- 1 until num_values) {
      log_deltas(u) = -math.log(u)
      for (i <- 0 to (u - 1)) {
        buff2(i) = log_deltas(i) + log_gammas(u - i)
      }
      log_deltas(u) += numerics.logSum(buff2, u + 1)
    }
    log_deltas
  }

  def zetaApproximation(ks: Array[Double], thetas: Array[Double], log_deltas: Array[Double], precision: Double): Double = {
    val log_rho = math.log(1.0 - thetas.min / thetas.max)
    val zeta_max = ks.sum
    val zeta_min = {
      val theta_min = thetas.min
      val idx = thetas.indexWhere(_ == theta_min)
      ks(idx)
    }

    def zetaApproxInner(lower: Double, upper: Double): Double = {
      require(lower <= upper)
      //      logInfo("bounds=(%f, %f, %f)" format (lower, upper, precision))
      if (upper <= lower + precision) {
        //        logInfo("returning %f" format (upper))
        upper
      } else {
        val middle = 0.5 * (lower + upper)
        var under_count = 0
        for (u <- 0 until log_deltas.size - 1) {
          val a = math.log(middle + u)
          val b = log_deltas(u + 1) - log_deltas(u) + math.log(u + 1) - log_rho
          if (a < b) {
            under_count += 1
          }
        }
        //          logInfo("zeta=%f, count=%d" format (middle, under_count))
        if (under_count < 0.5 * log_deltas.size) {
          zetaApproxInner(lower, middle)
        } else {
          zetaApproxInner(middle, upper)
        }
      }
    }
    zetaApproxInner(zeta_min, zeta_max)
  }

  def logDeltaApproximate(ks: Array[Double], thetas: Array[Double], num_values: Int, precompute_num_values: Int = 1000): Array[Double] = {
    if (num_values <= precompute_num_values) {
      return logDelta(ks, thetas, num_values)
    }
    val initial_log_deltas = logDelta(ks, thetas, precompute_num_values)
    val zeta = zetaApproximation(ks, thetas, initial_log_deltas, 1e-1)
    val complete_log_deltas = initial_log_deltas ++ Array.fill(num_values - precompute_num_values)(Double.NaN)
    val log_rho = math.log(1.0 - thetas.min / thetas.max)
    // Finish to fill using the approximation formula
    for (u <- precompute_num_values until num_values) {
      complete_log_deltas(u) = complete_log_deltas(u - 1) +
        log_rho +
        math.log(zeta + u - 1) - math.log(u)
    }
    complete_log_deltas
  }
}