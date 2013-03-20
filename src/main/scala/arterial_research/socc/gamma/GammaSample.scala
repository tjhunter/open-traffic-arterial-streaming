package arterial_research.socc.gamma

import cc.mallet.util.Randoms
import scala.{ math => m }
import org.apache.commons.math.special.Gamma
import breeze.stats.distributions.{ Gamma => GammaNLP }
import scala.collection.mutable.ArrayBuffer
import scalax.io.Resource
import core_extensions.MMLogging
import breeze.stats.distributions.Rand
import breeze.stats.distributions.Beta
import breeze.stats.distributions.Multinomial
import scala.annotation.tailrec
import breeze.numerics._
import breeze.linalg.DenseVector
import arterial_research.socc.PartialObservation
import arterial_research.socc.NodeId
import arterial_research.socc.CompleteObservation
import org.apache.commons.math.distribution.BetaDistributionImpl
import org.apache.commons.math.distribution.GammaDistributionImpl
import org.apache.commons.math.random.{ MersenneTwister => AMersenneTwister }
import cern.jet.random.{ Gamma => CGamma }
import cern.jet.random.engine.MersenneTwister
import org.apache.commons.math.random.RandomDataImpl
//import arterial_research.socc.gamma.GammaState

object GammaSample extends MMLogging {
  @transient val rand = Rand
  @transient private[this] val gen = new Randoms()
  val colt_gamma = new CGamma(1.0, 1.0, new MersenneTwister)
  // Make sure it is encapsulated in its own thread
  // It is not thread-safe
  val apache_random_local = new ThreadLocal[RandomDataImpl] {
    override def initialValue() = new RandomDataImpl(new AMersenneTwister(0))
  }
  def apache_random = apache_random_local.get()

  val min_t_chain = 1e-2

  /**
   * Sum of two numbers in the log domain
   */
  def logSumExp(lx: Double, ly: Double): Double = {
    if (lx < ly) {
      logSumExp(ly, lx)
    } else {
      // lx >= ly
      lx + math.log(1.0 + math.exp(ly - lx))
    }
  }

  def innerBeta(a: Double, b: Double): Double = {
    while (true) {
      val U = rand.uniform.draw()
      val V = rand.uniform.draw()
      if (U > 0 && V > 0) {
        // Performing the computations in the log-domain
        // The exponentiation may fail if a or b are really small
        //        val X = math.pow(U, 1.0 / a)
        val logX = math.log(U) / a
        //        val Y = math.pow(V, 1.0 / b)
        val logY = math.log(V) / b
        val logSum = logSumExp(logX, logY)
        if (logSum <= 0.0) {
          return math.exp(logX - logSum)
        }
      } else {
        logWarning("Found a zero: U = %f, V=%f" format (U, V))
      }
    }
    assert(false)
    0.0
  }

  def sampleBeta(a: Double, b: Double): Double = {
    val sample = if (a <= 1.0 && b <= 1.0) {
      innerBeta(a, b)
    } else {
      (new Beta(a, b)).draw()
      //        gen.nextBeta(a, b)
      // The beta sampler from math commons fails abysmally?????
      // (new BetaDistributionImpl(a, b)).sample()      
    }
    assert(sample >= 0, (sample, a, b))
    assert(sample <= 1, (sample, a, b))
    sample
  }

  /**
   * Correct but not particularly efficient implementation of the
   * sampling pair.
   */
  def sampleAuxi2(k1: Double, k2: Double, alpha: Double): Double = {
    val epsi_gamma = 1e-3
    if (m.abs(alpha) < epsi_gamma) {
      //      return (new Beta(k1, k2)).draw()
      //            return gen.nextBeta(k1, k2)
      return sampleBeta(k1, k2)
    }
    if (alpha < 0) {
      return 1.0 - sampleAuxi2(k2, k1, -alpha)
    }
    //    println("sampleAuxi2")
    // Find the maximum of the value.
    def fun(k: Int): Double = (Gamma.logGamma(k1 + k)
      - Gamma.logGamma(k1 + k2 + k)
      + k * m.log(alpha)
      - Gamma.logGamma(k))

    def gradFun(k: Int) = fun(k + 1) - fun(k)

    def findMax(min_search: Int, max_search: Int): Int = {
      assert(min_search <= max_search)
      if (min_search >= max_search - 2) {
        min_search
      } else {
        val x = (min_search + max_search) / 2
        if (gradFun(x) > 0) {
          findMax(x, max_search)
        } else {
          findMax(min_search, x)
        }
      }
    }

    // Compute the maximum index
    val k_max = {
      if (gradFun(1) <= 0) {
        1
      } else {
        var k_neg = 1
        while (gradFun(k_neg) >= 0) {
          k_neg *= 2
        }
        findMax(math.max(1, k_neg / 2), math.max(1, k_neg))
      }
    }
    // Compute the range around the values
    val k_max_val = fun(k_max)
    assert(!k_max_val.isInfinite, (k1, k2, alpha, k_max))
    //    logInfo("kmax: %d %f" format (k_max, k_max_val))
    val decrease_factor = -20
    val up_vals = {
      val ups_vals = new ArrayBuffer[Double]()
      ups_vals.append(k_max_val)
      var k_up = k_max + 1
      var k_up_val = fun(k_up)
      while (k_up_val >= k_max_val + decrease_factor) {
        //        println("up %d %f" format (k_up, k_up_val))
        ups_vals.append(k_up_val)
        k_up += 1
        k_up_val = fun(k_up)
      }
      ups_vals.toArray
    }
    val down_vals = {
      val downs_vals = new ArrayBuffer[Double]()
      var k_down = k_max - 1
      var k_down_val = fun(k_down)
      while (k_down > 0 && k_down_val >= k_max_val + decrease_factor) {
        downs_vals.append(k_down_val)
        k_down -= 1
        k_down_val = fun(k_down)
        //        println("down %d %f" format (k_down, k_down_val))
      }
      downs_vals.reverse.toArray
    }
    val log_ys = (down_vals ++ up_vals).toArray
    val zs = {
      val exp_ys = log_ys.map(y => m.exp(y - k_max_val))
      val sum_exp_ys = exp_ys.sum
      exp_ys.map(_ / sum_exp_ys)
    }
    //    println("zs: "+zs.mkString(" "))
    //    println("k_max "+k_max)
    //    println("down_vals.size "+down_vals.size)
    // Forgot this -1 here :(
    val padding = k_max - down_vals.size - 1
    //    logInfo("ys(Auxi) "+((0 until padding).map(i=>0)++log_ys))
    val k = (new Multinomial(new DenseVector(zs))).sample() + padding
    //    val s = (new Beta(k1 + k, k2)).sample()
    //    val s = (new Beta(k1 + k, k2)).sample()
    //            val s = gen.nextBeta(k1+k, k2)
    val s = sampleBeta(k1 + k, k2)
    assert(s <= 1, (s, s - 1.0, k1 + k, k2))
    assert(s >= 0, (s, s - 1.0, k1 + k, k2))
    s
  }

  def sampleState(gs: GammaState): Double = {
    // Watch out for Colt: uses the alpha/beta convention
//    val apache_random = new RandomDataImpl(new AMersenneTwister()) // The MersenneTwister class is not thread safe
    val threshold = 1e-5
    var x = -1.0
    while (x <= threshold) {
      x = apache_random.nextGamma(gs.k, gs.theta)
      //      x = colt_gamma.nextDouble(gs.k, 1.0 / gs.theta)
      //      x = gen.nextGamma(gs.k, gs.theta)
      //      try {
      //        x = apache_random.nextGamma(gs.k, gs.theta)
      //      } catch {
      //        case x: ArrayIndexOutOfBoundsException => {
      //          logWarning("Failure in apache: gs = %s, reason=%s" format (gs.toString, x.getMessage()))
      //          throw x
      //        }
      //      }

    }
    x
    //    gen.nextGamma(gs.k, gs.theta)
  }

  def sampleState(gs: GammaState, num_elements: Int): Array[Double] = {
    val res = Array.fill(num_elements)(0.0)
    val threshold = 1e-5
    for (idx <- 0 until num_elements) {
      var x = -1.0
      while (x <= threshold) {
        x = apache_random.nextGamma(gs.k, gs.theta)
      }
      res(idx) = x
    }
    res
  }

  //  def sampleState(gs: GammaState): Double = {
  //    gen.nextGamma(gs.k, gs.theta)
  //  }

  //  def sample_from_probs(ps: Array[Double]): Int = {
  //    val u = m.random
  //    var cum = 0.0
  //    var i = 0
  //    while (i < ps.size - 1) {
  //      cum += ps(i)
  //      if (u < cum) {
  //        return i
  //      }
  //      i += 1
  //    }
  //    return ps.size - 1
  //  }

  val max_range = 50

  def sample_auxi(alpha: Double, beta: Double, gamma: Double): Double = {
    val epsi_gamma = 1e-3
    if (m.abs(gamma) < epsi_gamma) {
      //      return (new Beta(alpha, beta)).draw()
      //      return (new BetaDistributionImpl(alpha, beta)).sample()
      //      return gen.nextBeta(alpha, beta)
      return sampleBeta(alpha, beta)
    }
    if (gamma < 0) {
      return 1 - sample_auxi(beta, alpha, -gamma)
    }
    val ys = new Array[Double](max_range)
    var ys_max: Double = -100000
    for (i <- 0 until max_range) {
      val k = i + 1
      val y = (Gamma.logGamma(alpha + k)
        - Gamma.logGamma(alpha + beta + k)
        + k * m.log(gamma)
        - Gamma.logGamma(k))

      ys(i) = y
      ys_max = m.max(y, ys_max)
    }
    //    println("ys(auxi):"+(List(0)++ys).mkString(" "))
    //    println(ys mkString "\n")
    val exp_ys = ys.map(y => m.exp(y - ys_max))
    val sum_exp_ys = exp_ys.sum
    val normed_exp_ys = exp_ys.map(_ / sum_exp_ys)
    val k = (new Multinomial(new DenseVector(normed_exp_ys))).sample()
    //    (new BetaDistributionImpl(alpha+k, beta)).sample()
    //    (new Beta(alpha+k, beta)).draw()
    sampleBeta(alpha + k, beta)
    //    gen.nextBeta(alpha + k, beta)
  }

  def sampler_pair(gs1: GammaState, gs2: GammaState, tt: Double): (Double, Double) = {
    //     require(tt >= min_t_chain)
    require(gs1.k > 0)
    require(gs2.k > 0)
    val theta_inv = tt * (1.0 / gs2.theta - 1.0 / gs1.theta)
    //    println(theta_inv)
    val u = sample_auxi(gs1.k, gs2.k, theta_inv)
    //    val u = sampleAuxi2(gs1.k, gs2.k, theta_inv)
    //    GammaDis2.sampleAuxi2(gs1.k, gs2.k, theta_inv)
    assert(u >= 0, u)
    assert(u <= 1, u)
    //    assert(false)
    return (tt * u, tt * (1 - u))
  }

  def getSamples(p_obs: PartialObservation,
    state: Map[NodeId, GammaState],
    num_samples: Int, burnin: Int = 1000): Seq[CompleteObservation] = {
    require(state != null)
    require(p_obs.mask != null)
    val states = p_obs.mask.nodes.map(state apply _) toArray

    // In this case, no need to do sampling, we are conditionned on a single element.
    if (states.size == 1) {
      return (0 until num_samples).map(i => {
        new CompleteObservation(p_obs.mask, Array(p_obs.total))
      })
    }

    // Building start state of the chain
    val means = states.map(s => s.k * s.theta)
    val chain_state = means.map(_ * p_obs.total / means.sum)
    val samples = new ArrayBuffer[Array[Double]]
    // Running the chain
    for (epoch <- 0 until (burnin + num_samples)) {
      for (i <- 0 until states.size) {
        val j = (i + 1) % states.size
        val gs1 = states(i)
        val gs2 = states(j)
        val tt = chain_state(i) + chain_state(j)
        val (u, v) = sampler_pair(gs1, gs2, tt)
        if (u <= min_t_chain) {
          //           logWarning("Extreme element u in chain: state1=%s, state2=%s, tt=%f, u=%f" format (gs1.toString, gs2.toString, tt, u))
        }
        if (v <= min_t_chain) {
          //           logWarning("Extreme element v in chain: state1 = %s, state2 = %s, tt = %f, v = %f" format (gs1.toString, gs2.toString, tt, v))
        }
        chain_state(i) = u
        chain_state(j) = v
      }
      if (epoch >= burnin) {
        // This looks like a bug...
        if (chain_state.exists(_ <= min_t_chain)) {
          //           logWarning("Negative elements found in chain\nstates=" + states.mkString(" ")+" tt = "+p_obs.total)
        } else {
          samples.append(chain_state clone)
        }
      }
    }
    samples.map(new CompleteObservation(p_obs.mask, _)).toArray.toSeq
  }

}