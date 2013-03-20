package arterial_research.socc
import spark._
import spark.SparkContext._
import core_extensions.MMLogging
import spark.broadcast.Broadcast
import arterial_research.socc.io.DataSliceIndex
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.gamma.GammaSample
import arterial_research.socc.gamma.GammaLearn
import arterial_research.socc.gamma.GammaFunctions
import gamma.GammaMixture
import arterial_research.socc.gamma.GammaDirichlet

class StepStatistics(
  var step: Int = -1,
  var state: Map[NodeId, GammaState] = null,
  var full_state: Map[NodeId, GammaState] = null,
  var observed_weights: Map[NodeId, Double] = null,
  var total_time: Double = 0.0,
  var slice_index: DataSliceIndex = null,
  var marginal_ll: Double = 0.0,
  var entropy: Double = 0.0,
  var ecll: Double = 0.0,
  var post_ecll: Double = 0.0,
  var post_entropy: Double = 0.0,
  var obs_count: Int = 0) extends Serializable {
  def stringRepr = {
    ("step = %d\n" format step.toInt) +
      ("total_time = %s\n" format total_time.toString())
  }
}

object LearnSpark extends MMLogging {

  def completeLogLikelihood(obs: CompleteObservation, state: Map[NodeId, GammaState]): Double = {
    (obs.partial zip obs.mask.nodes).map({ case (x, nid) => GammaFunctions.logLikelihood(state(nid), x) }).sum
  }

  def marginalLogLikelihood(obs: PartialObservation, state: Map[NodeId, GammaState]): Double = {
    val obs_states = obs.mask.nodes.map(state.apply _)
    //        GammaMixture.sumOfGamma(obs_states).logPdf(obs.total)
    GammaMixture.positiveCombinationOfGammas(obs_states, obs.mask.weights).logPdf(obs.total)
  }

  def learnWeightedWithSparkStep(
    p_obs: RDD[(PartialObservation, Double)],
    state: Map[NodeId, GammaState],
    em_context: EMContext,
    current_slice: DataSliceIndex,
    current_inner_step: Int,
    context: SparkContext): (Map[NodeId, GammaState], StepStatistics) = {

    val bc_em_context = context.broadcast(em_context)
    // Collect all the stats
    val stats = new StepStatistics()
    stats.slice_index = current_slice
    stats.step = current_inner_step
    stats.obs_count = p_obs.count().toInt
    // Broadcast the new state
    val bc_state = context.broadcast(state)

    // Compute the marginal log likelihood
    val weighted_marginal_lls = p_obs.map({
      case (p_ob, w) =>
        (w, w * marginalLogLikelihood(p_ob, bc_state.value))
    }).reduce(mean_reducer)

    val marginal_ll = {
      val (all_weights, all_marginal_lls) = weighted_marginal_lls
      all_marginal_lls / all_weights
    }
    logInfo("Marginal LL: " + marginal_ll)
    stats.marginal_ll = marginal_ll

    // Building the complete samples by MCMC
    val em_complete_samples = p_obs.flatMap({
      case (p_ob, w) =>
        val s = bc_state.value
        val complete_samples = GammaSample.getSamples(p_ob, s, bc_em_context.value.config.num_em_samples)
        complete_samples.map((_, w))
    })

    // The ECLL before learning
    val weighted_ec_lls = em_complete_samples.map({
      case (obs, w) =>
        (w, w * completeLogLikelihood(obs, bc_state.value))
    }).reduce(mean_reducer)

    val ec_ll = {
      val (all_weights, all_ec_lls) = weighted_ec_lls
      all_ec_lls / all_weights
    }
    logInfo("ECLL: " + ec_ll)
    stats.ecll = ec_ll

    // Shuffling
    val samples_by_node = em_complete_samples
      .flatMap({
        case (cs, w) =>
          (cs.mask.nodes zip cs.partial).map(z => (z._1, z._2, w))
      })
      .groupBy(_._1).map(pair => {
        val (nid, vals) = pair
        val val_arr = vals.map(z => (z._3, z._2)).toArray
        (nid, val_arr)
      })
    // Computing the new distribution
    val new_node_states = samples_by_node.map(z => {
      val (nid, values) = z
      val prior_strength = bc_em_context.value.config.smoothing_strength
      val prior = bc_em_context.value.prior(nid)
      val tts = values.map(_._2)
      val ws = values.map(_._1)
      // New procedure seems to work?
      val new_state = GammaLearn.mle2(tts, ws, prior, prior_strength)
      //      val new_state = GammaLearn.mle(tts, ws, prior, prior_strength)
      (nid, new_state)
    })
    // The new state
    logInfo("Before collect")
    val new_state = new_node_states.collect.toMap
    // Make sure to complete with all the nodes
    val complete_new_state = new_state ++ state.toSeq.filterNot(z => new_state.contains(z._1))

    // Broadcast the new state
    val bc_complete_new_state = context.broadcast(complete_new_state)

    // The ECLL after learning
    val weighted_postec_lls = em_complete_samples.map({
      case (obs, w) =>
        (w, w * completeLogLikelihood(obs, bc_complete_new_state.value))
    }).reduce(mean_reducer)

    val postec_ll = {
      val (all_weights, all_postec_lls) = weighted_postec_lls
      all_postec_lls / all_weights
    }
    logInfo("Post-learning ECLL: " + postec_ll)
    stats.post_ecll = postec_ll

    // The stats will only output what got modified
    stats.state = new_state
    stats.full_state = complete_new_state
    (complete_new_state, stats)
  }

  /**
   * Spark implementation of the algorithm.
   */
  def learnWeightedStepSpark(
    p_obs: RDD[(Double, PartialObservation)],
    state: Map[NodeId, GammaState],
    em_context: EMContext,
    current_slice: DataSliceIndex,
    current_inner_step: Int,
    context: SparkContext): (Map[NodeId, GammaState], Map[NodeId, GammaState], StepStatistics) = {

    //    logInfo("Weights:"+p_obs.map(_._1).collect().mkString(" "))
    val bc_em_context = context.broadcast(em_context)
    // Collect all the stats
    val stats = new StepStatistics()
    stats.slice_index = current_slice
    stats.step = current_inner_step
    stats.obs_count = p_obs.count().toInt
    // Broadcast the new state
    val bc_state = context.broadcast(state)

    {
      // Record the number of observations crossing each link
      val weighted_counts = p_obs.flatMap({
        case (w, p_ob) =>
          p_ob.mask.nodes.map((w, _))
      }).groupBy(_._2)
        .mapValues(_.map(_._1).sum)
        .reduceByKey(_ + _)
        .collect
        .toMap
      stats.observed_weights = weighted_counts
    }

    // Compute the marginal log likelihood
    logInfo("Computing marginal ll...")
    val weighted_marginal_lls = p_obs.map({
      case (w, p_ob) =>
        (w, w * marginalLogLikelihood(p_ob, bc_state.value))
    }).reduce(mean_reducer)

    val marginal_ll = {
      val (all_weights, all_marginal_lls) = weighted_marginal_lls
      all_marginal_lls / all_weights
    }
    logInfo("Marginal LL: " + marginal_ll)
    stats.marginal_ll = marginal_ll

    // For each observation, create a set of sample
    // We leave the observation associated with the set of samples for now
    // It will be useful to compute the marginal LL
    val num_em_samples: Int = em_context.config.num_em_samples
    val em_grouped_samples = p_obs.map({
      case (w, p_ob) =>
        val gss = p_ob.mask.nodes.map(bc_state.value.apply _)
        val t = p_ob.total
        val alphas = p_ob.mask.weights
        val p_ob_samples = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_em_samples)
        (w, p_ob, p_ob_samples)
    })

    // Compute the ECLL and the entropy
    // The sum should equal the marginal LL
    {
      logInfo("Computing ecll...")
      val weighted_empirical_marginal_lls = em_grouped_samples.map({
        case (w, p_ob, p_ob_samples) =>
          val gss = p_ob.mask.nodes.map(bc_state.value.apply _)
          val t = p_ob.total
          val alphas = p_ob.mask.weights
          val (ecll, entropy) = GammaDirichlet.ECLLEntropy(p_ob_samples, gss, alphas, t)
          if (ecll.isInfinite() || ecll.isNaN() || entropy.isInfinite() || entropy.isNaN()) {
            logWarning("Found bad value: ecll=%s, entropy=%s, gss=%s,alphas=%s,t=%s" format (
              ecll.toString(),
              entropy.toString(),
              gss.mkString(" "),
              alphas.mkString(" "),
              t.toString()))
            logInfo("Samples:\n%s" format (p_ob_samples.map(_.mkString(" ")).mkString("\n")))
          }
          (w, w * ecll, w * entropy)
      }).reduce(ecll_reducer)
      val (empirical_ecll, empirical_entropy) = {
        val (w, wecll, wentropy) = weighted_empirical_marginal_lls
        (wecll / w, wentropy / w)
      }
      stats.ecll = empirical_ecll
      stats.entropy = empirical_entropy
      logInfo("empirical ECLL: %f empirical entropy: %f => empirical marginal LL: %f" format (empirical_ecll, empirical_entropy, empirical_ecll + empirical_entropy))
    }

    val em_complete_samples = em_grouped_samples.flatMap({
      case (w, p_ob, p_ob_samples) =>
        p_ob_samples.map(x => {
          val obs = CompleteObservation(p_ob.mask, x)
          (w, obs)
        })
    })

    // Shuffling
    logInfo("Shuffling...")
    val samples_by_node = em_complete_samples
      .flatMap({
        case (w, cs) =>
          (cs.mask.nodes zip cs.partial).map(z => (z._1, z._2, w))
      })
      .groupBy(_._1).map(pair => {
        val (nid, vals) = pair
        val val_arr = vals.map(z => (z._3, z._2)).toArray
        (nid, val_arr)
      })

    // Computing the new distribution
    logInfo("Learning...")
    val new_node_states = samples_by_node.map(z => {
      val (nid, values) = z
      val smoothing_strength = bc_em_context.value.config.smoothing_strength
      val prior = bc_em_context.value.prior(nid)
      //      val prior = priors(nid)
      val tts = values.map(_._2)
      val ws = values.map(_._1)
      //      logInfo("Learning for link " + nid)
      // New procedure seems to work?
      val new_state = GammaLearn.mle2(tts, ws, prior, smoothing_strength)
      //      val new_state = GammaLearn.mle(tts, ws, prior, smoothing_strength)
      (nid, new_state)
    })
    // The new state
    logInfo("Before collect")
    val new_state = new_node_states.collect.toMap
    // Make sure to complete with all the nodes
    val complete_new_state = new_state ++ state.toSeq.filterNot(z => new_state.contains(z._1))

    //    // Compute the ECLL and the entropy
    //    // The sum should equal the marginal LL
    //    {
    //      logInfo("Computing post ecll...")
    //      val weighted_empirical_marginal_lls = em_grouped_samples.map({
    //        case (w, p_ob, p_ob_samples) =>
    //          val gss = p_ob.mask.nodes.map(complete_new_state.apply _)
    //          val t = p_ob.total
    //          val alphas = p_ob.mask.weights
    //          val (ecll, entropy) = GammaDirichlet.ECLLEntropy(p_ob_samples, gss, alphas, t)
    //          (w, w * ecll, w * entropy)
    //      }).reduce(ecll_reducer)
    //      val (empirical_ecll, empirical_entropy) = {
    //        val (w, wecll, wentropy) = weighted_empirical_marginal_lls
    //        (wecll / w, wentropy / w)
    //      }
    //      stats.post_ecll = empirical_ecll
    //      stats.post_entropy = empirical_entropy
    //      logInfo("POST LEARNING: empirical ECLL: %f empirical entropy: %f => empirical marginal LL: %f" format (empirical_ecll, empirical_entropy, empirical_ecll + empirical_entropy))
    //    }

    // The stats will only output what got modified
    (new_state, complete_new_state, stats)
  }

  def mean_reducer(p1: (Double, Double), p2: (Double, Double)) = (p1._1 + p2._1, p1._2 + p2._2)

  def ecll_reducer(p1: (Double, Double, Double), p2: (Double, Double, Double)) = (p1._1 + p2._1, p1._2 + p2._2, p1._3 + p2._3)

  /**
   * The reference algorithm. Does not use spark.
   */
  def learnWeightedStep(
    p_obs: Seq[(Double, PartialObservation)],
    state: Map[NodeId, GammaState],
    num_em_samples: Int,
    smoothing_strength: Double,
    priors: Map[NodeId, GammaState]): Map[NodeId, GammaState] = {

    // Compute the marginal log likelihood
    logInfo("Computing marginal ll...")
    val weighted_marginal_lls = p_obs.map({
      case (w, p_ob) =>
        // FIXME should not be log
        (w, w * marginalLogLikelihood(p_ob, state))
    }).reduce(mean_reducer)

    val marginal_ll = {
      val (all_weights, all_marginal_lls) = weighted_marginal_lls
      all_marginal_lls / all_weights
    }
    logInfo("Marginal LL: " + marginal_ll)

    // For each observation, create a set of sample
    // We leave the observation associated with the set of samples for now
    // It will be useful to compute the marginal LL
    val em_grouped_samples = p_obs.map({
      case (w, p_ob) =>
        val gss = p_ob.mask.nodes.map(state.apply _)
        val t = p_ob.total
        val alphas = p_ob.mask.weights
        val p_ob_samples = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_em_samples)
        (w, p_ob, p_ob_samples)
    })

    // Compute the ECLL and the entropy
    // The sum should equal the marginal LL
    {
      logInfo("Computing empirical ECLL...")
      val weighted_empirical_marginal_lls = em_grouped_samples.map({
        case (w, p_ob, p_ob_samples) =>
          val gss = p_ob.mask.nodes.map(state.apply _)
          val t = p_ob.total
          val alphas = p_ob.mask.weights
          val (ecll, entropy) = GammaDirichlet.ECLLEntropy(p_ob_samples, gss, alphas, t)
          (w, w * ecll, w * entropy)
      }).reduce(ecll_reducer)
      val (empirical_ecll, empirical_entropy) = {
        val (w, wecll, wentropy) = weighted_empirical_marginal_lls
        (wecll / w, wentropy / w)
      }
      logInfo("empirical ECLL: %f empirical entropy: %f => empirical marginal LL: %f" format (empirical_ecll, empirical_entropy, empirical_ecll + empirical_entropy))
    }

    val em_complete_samples = em_grouped_samples.flatMap({
      case (w, p_ob, p_ob_samples) =>
        p_ob_samples.map(x => {
          val obs = CompleteObservation(p_ob.mask, x)
          (w, obs)
        })
    })

    // Shuffling
    val samples_by_node = em_complete_samples
      .flatMap({
        case (w, cs) =>
          (cs.mask.nodes zip cs.partial).map(z => (z._1, z._2, w))
      })
      .groupBy(_._1).map(pair => {
        val (nid, vals) = pair
        val val_arr = vals.map(z => (z._3, z._2)).toArray
        (nid, val_arr)
      })

    // Computing the new distribution
    val new_node_states = samples_by_node.map(z => {
      val (nid, values) = z
      val prior = priors(nid)
      val tts = values.map(_._2)
      val ws = values.map(_._1)
      logInfo("Learning for link " + nid)
      // New procedure seems to work?
      val new_state = GammaLearn.mle2(tts, ws, prior, smoothing_strength)
      //      val new_state = GammaLearn.mle(tts, ws, prior, smoothing_strength)
      (nid, new_state)
    })
    // The new state
    logInfo("Before collect")
    val new_state = new_node_states
    // Make sure to complete with all the nodes
    val complete_new_state = new_state ++ state.toSeq.filterNot(z => new_state.contains(z._1))

    // Compute the ECLL and the entropy
    // The sum should equal the marginal LL
    {
      val weighted_empirical_marginal_lls = em_grouped_samples.map({
        case (w, p_ob, p_ob_samples) =>
          val gss = p_ob.mask.nodes.map(complete_new_state.apply _)
          val t = p_ob.total
          val alphas = p_ob.mask.weights
          val (ecll, entropy) = GammaDirichlet.ECLLEntropy(p_ob_samples, gss, alphas, t)
          (w, w * ecll, w * entropy)
      }).reduce(ecll_reducer)
      val (empirical_ecll, empirical_entropy) = {
        val (w, wecll, wentropy) = weighted_empirical_marginal_lls
        (wecll / w, wentropy / w)
      }
      logInfo("POST LEARNING: empirical ECLL: %f empirical entropy: %f => empirical marginal LL: %f" format (empirical_ecll, empirical_entropy, empirical_ecll + empirical_entropy))
    }

    // The stats will only output what got modified
    complete_new_state
  }
}
