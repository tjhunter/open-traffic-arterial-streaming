package arterial_research.socc.gamma
import breeze.linalg._
import breeze.linalg.DenseVector._
import org.apache.commons.math.special.Gamma
import core_extensions.MMLogging
import scala.{ math => m }
//import breeze.numerics

/**
 * The Gamma-Dirichlet distribution, a generalization
 * of the dirichlet distribution
 */
class GammaDirichlet(gss: Array[GammaState]) extends MMLogging {
  val gammas = gss
  lazy val ks = DenseVector(gammas.map(_.k): _*)
  lazy val thetas = DenseVector(gammas.map(_.theta): _*)
  lazy val logNorm = {
    val ln = GammaDirichlet.normalizationFactorSecure(ks.data, thetas.data)
    //    math.log(GammaDirichlet.normalizationFactor(ks.data, thetas.data))
    if (ln.isInfinite() || ln.isNaN()) {
      logWarning("normalization factor is ill-defined: ln=%f, ks=%s, thetas=%s" format (ln, ks.data.mkString(" "), thetas.data.mkString(" ")))
    }
    ln
  }

  def unnormalizedLL(x: Array[Double]): Double = {
    gammas.zip(x).map(z => {
      val ll = GammaFunctions.logLikelihood(z._1, z._2)
      if (ll.isNaN() || ll.isInfinite()) {
        logWarning("log-likelihood is ill-defined: ll = %f, z=%f, gamma=%s" format (ll, z._2, z._1.toString()))
      }
      ll
    }).sum
  }

  def sample(num_samples: Int): Array[Array[Double]] = {
    (0 until num_samples).map(i => {
      val zs = gammas.map(GammaSample.sampleState(_))
      val zs_sum = zs.sum
      for (idx <- 0 until zs.size) {
        zs(idx) /= zs_sum
      }
      zs
    }).toArray
  }
  
  def sample1(num_samples: Int): Array[Array[Double]] = {
    // Sampling by column is more efficient for the sampler
    val ys = gammas.map(GammaSample.sampleState(_, num_samples))
    val n = gammas.size
    (0 until num_samples).map(i => {
      val zs = Array.fill(n)(0.0)
      for (idx <- 0 until n) {
        zs(idx) = ys(idx)(i)
      }
      val zs_sum = zs.sum
      for (idx <- 0 until zs.size) {
        zs(idx) /= zs_sum
      }
      zs
    }).toArray
  }
  

  def ll(x: Array[Double]): Double = {
    unnormalizedLL(x) - logNorm
  }

  def ll(xs: Array[Array[Double]]): Array[Double] = {
    xs.map(ll(_))
  }
}

object GammaDirichlet extends MMLogging {
  def normalizationFactor(ks: Array[Double], thetas: Array[Double]): Double = {
    //    logInfo("In standard normalization")
    require(ks.size == thetas.size)
    require(ks.forall(_ >= min_k_value))
    require(thetas.forall(_ >= min_theta_value))

    val thetas_ = DenseVector(thetas)
    val ks_ = DenseVector(ks)

    val rho = ks.sum
    val theta_max = thetas.max
    val theta_min = thetas.min
    val num_iters = 500
    val gammas = (0 until num_iters).map(l => {
      val p = ((thetas_ :^ -1.0) * (-theta_min) + 1.0) :^ l.toDouble
      //      logInfo("p:"+p.data.mkString(" "))
      ks_.dot(p)
    }).toArray
    val gammas_tilde = DenseVector(gammas)
    val deltas = (0 until num_iters).map(z => 0.0).toArray
    deltas(0) = 1.0
    for (u <- 1 until num_iters) {
      for (l <- 1 until (u + 1)) {
        deltas(u) += gammas_tilde(l) * deltas(u - l) / u
      }
    }
    val C_tilde = (thetas_ :^ (-ks_)).valuesIterator.toArray.product * math.exp(-1.0 / theta_min)
    //    logInfo("C_tilde: " + C_tilde)
    //    logInfo("gammas " + gammas.mkString(" "))
    //    logInfo("deltas " + deltas.mkString(" "))
    val us = DenseVector.tabulate(num_iters)(_.toDouble)
    val vals = (0 until num_iters).map(u => {
      deltas(u) * math.pow(theta_min, -us(u)) / math.exp(Gamma.logGamma(rho + us(u)))
    })
    //    logInfo("zs " + vals.mkString(" "))
    //    logInfo("Exiting standard normalization")
    C_tilde * vals.sum
  }

  /**
   * A robust alternative to the computation of the normalization factor.
   */
  def normalizationFactorSecure(ks: Array[Double], thetas: Array[Double]): Double = {
    require(ks.size == thetas.size)
    require(ks.forall(_ >= min_k_value))
    require(thetas.forall(_ >= min_theta_value))
    // Using for now the regular implementation
    val gss = (ks zip thetas).map(z => GammaState(z._1, z._2))
    GammaMixture.sumOfGammaAdaptive(gss).logPdf(1.0)

    //    val factor = 12
    //    val max_values = 20000
    //    val delta_infinite_approx = 500
    //
    //    val thetas_ = DenseVector(thetas)
    //    val ks_ = DenseVector(ks)
    //    val log_thetas = thetas.map(m.log _)
    //    val log_ks = ks.map(m.log _)
    //    val n = thetas.size
    //
    //    // Constants
    //    val rho = ks.sum
    //    val theta_max = thetas.max
    //    val theta_min = thetas.min
    //    val log_C_tilde = {
    //      var x = -1.0 / theta_min
    //      for (idx <- 0 until n) {
    //        x += log_thetas(idx) * (-ks(idx))
    //      }
    //      x
    //    }
    //    val ks_max = ks.max
    //    //    logInfo("C_tilde: " + m.exp(log_C_tilde))
    //
    //    // A special check when all the values are equal
    //    if (theta_min == theta_max) {
    //      return log_C_tilde - Gamma.logGamma(rho)
    //    }
    //
    //    // The buffers
    //    val log_gammas = Array.fill(max_values)(Double.NaN)
    //    val log_deltas = Array.fill(max_values)(Double.NaN)
    //    val log_zs = Array.fill(max_values)(Double.NegativeInfinity)
    //
    //    // Variables
    //    var current_idx = 0
    //    var current_z_max = Double.NegativeInfinity
    //
    //    // Initialize the variables
    //    log_deltas(0) = 0.0
    //    log_gammas(0) = m.log(rho)
    //    log_zs(0) = -Gamma.logGamma(rho)
    //    current_z_max = log_zs(0)
    //
    //    val buff = Array.fill(n)(Double.NaN)
    //    val buff2 = Array.fill(max_values)(Double.NaN)
    //    // Compute the values
    //    while (log_zs(current_idx) >= current_z_max - factor && current_idx <= max_values - 3) {
    //      val u = current_idx + 1
    //      //      logInfo("u = %d, log_z=%f" format (u, log_zs(current_idx)))
    //      // Compute the next value of gamma      
    //      log_gammas(u) = {
    //        val l = current_idx + 1
    //        for (idx <- 0 until n) {
    //          buff(idx) = l * m.log(1.0 - theta_min / thetas(idx)) + log_ks(idx)
    //        }
    //        //        logInfo("buff = " + buff.map(m.exp _).mkString(" "))
    //        numerics.logSum(buff)
    //      }
    //      //      logInfo("log_gammas(u) = " + log_gammas(u))
    //      // Compute the next value of delta
    //      log_deltas(u) = {
    //        if (u > delta_infinite_approx) {
    //          Gamma.logGamma(u + ks_max) - Gamma.logGamma(u + delta_infinite_approx)
    //          -Gamma.logGamma(u) + Gamma.logGamma(delta_infinite_approx)
    //          +(u - delta_infinite_approx) * m.log(1 - theta_min / theta_max)
    //          +log_deltas(delta_infinite_approx)
    //        } else {
    //          buff2(0) = Double.NegativeInfinity
    //          for (l <- 1 until (u + 1)) {
    //            buff2(l) = log_gammas(l) + log_deltas(u - l) - m.log(u)
    //          }
    //          numerics.logSum(buff2, u + 1)
    //        }
    //      }
    //      //      logInfo("log_deltas(u) = " + log_deltas(u))
    //      // Compute the next value of z
    //      log_zs(u) = {
    //        log_deltas(u) - u * m.log(theta_min) - Gamma.logGamma(rho + u)
    //      }
    //      //      logInfo("log_zs(u) = " + log_zs(u))
    //      // Increment
    //      current_idx += 1
    //      if (log_zs(current_idx) > current_z_max) {
    //        current_z_max = log_zs(current_idx)
    //      }
    //    }
    //
    //    // Go back to remove the first elements that may be too small as well.
    //    var start_idx = current_idx - 1
    //    while (start_idx > 0 && log_zs(start_idx) >= current_z_max - factor) {
    //      start_idx -= 1
    //    }
    //    //    logInfo("start_idx: "+start_idx)
    //    numerics.logSum(log_zs.slice(start_idx, current_idx)) + log_C_tilde
  }

  def sampleFromScaled(gss: Array[GammaState], t: Double, alphas: Array[Double], num_samples: Int): Array[Array[Double]] = {
    require(alphas.min >= min_alpha_value)
    val alphas_ = DenseVector(alphas)
    val scaled_gss = alphas.zip(gss).map({
      case (alpha, gs) =>
        GammaState(gs.k, gs.theta).scaled(alpha / t)
    })
    val gd = new GammaDirichlet(scaled_gss)
    val samples = gd.sample(num_samples)
    samples.map(x_ => {
      val x = DenseVector(x_)
      x :/= alphas_
      x :*= t
      x.data
    }).toArray
  }

  def ECLLEntropy(samples: Array[Array[Double]], gss: Array[GammaState], alphas: Array[Double], t: Double): (Double, Double) = {
    val gd = new GammaDirichlet(gss)
    val alphas_ = DenseVector(alphas)
    val n = samples.size
    val ecll = samples.map(x => {
      val ll = gd.unnormalizedLL(x)
      if (ll != ll || ll < -1e10) {
        logInfo("Computing LL failed for entropy: ll = " + ll + ("gss = %s, alphas = %s" format (gss.mkString(" "), alphas.mkString(" "))) + ("  Sample:%s" format (x.mkString(" "))))
      }
      ll
    }).sum / n
    val scaled_gss = (alphas.zip(gss)).map({
      case (alpha, gs) =>
        GammaState(gs.k, alpha * gs.theta / t)
    }).toArray
    val sgd = new GammaDirichlet(scaled_gss)
    val u = (1.0 - 1.0 / alphas.size) * alphas.map(alpha => math.log(t / alpha)).sum
    val entropy = -samples.map(x => {
      val ll = sgd.ll((DenseVector(x) :* (alphas_) / t).data)
      if (ll != ll || ll < -1e10) {
        logInfo("Computing LL failed for entropy: ll = " + ll + ("gss = %s, alphas = %s" format (gss.mkString(" "), alphas.mkString(" "))) + ("  Sample:%s" format (x.mkString(" "))))
      }
      ll
    }).sum / n + u
    (ecll, entropy)
  }

  val min_k_value = 1e-5
  val min_theta_value = 1e-5
  val min_alpha_value = 1e-2
}