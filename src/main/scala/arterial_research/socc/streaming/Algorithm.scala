package arterial_research.socc.streaming
import arterial_research.socc.EMConfig
import scala.Array.canBuildFrom
import com.twitter.util.Config.fromOption
import com.twitter.util.Config.fromRequired
import arterial_research.socc.LearnSpark.completeLogLikelihood
import arterial_research.socc.LearnSpark.marginalLogLikelihood
import arterial_research.socc.LearnSpark.mean_reducer
import arterial_research.socc.gamma.GammaLearn
import arterial_research.socc.gamma.GammaSample
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.NodeId
import arterial_research.socc.StepStatistics
import arterial_research.socc.InputOutput
import arterial_research.socc.LearnSpark
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.EMContext
import arterial_research.socc.PartialObservation
import arterial_research.socc.SparkConfig
import spark.streaming.StreamingContext.toPairDStreamFunctions
import spark.streaming.DStream
import spark.streaming.Seconds
import spark.streaming.{Duration => SDuration}
import spark.streaming.StreamingContext
import core_extensions.MMLogging
import arterial_research.socc.Environment
import scala.actors.Future
import scala.actors.Futures
import spark.SparkEnv
import spark.streaming.Time
import org.joda.time.DateTime
import spark.SparkContext
import arterial_research.socc.io.DataSliceIndex
import java.io.File
import scala.util.control.Breaks._
import spark.streaming.Duration

/**
 * The main algorithm that handles the computations.
 */
object Algorithm extends MMLogging {

  def countElements(em_config: EMConfig): Unit = {
    // Creates the spark context
    val master_url:String = em_config.spark.master_url
    val framework:String = em_config.spark.framework
    val dt:Duration = em_config.data_description.streaming_chunk_length
    val ssc = new StreamingContext(master_url, framework, dt)

    val start_data_slice_index = ExperimentFunctions.startDataSliceIndex(em_config)

    // Build the stream
    // Watch out: since we are calling historical data on a real-time system, we need
    // to shift the clocks.
    val data_stream = ExperimentFunctions.createDStreamFromConfig(ssc, em_config)
//    logInfo("number of streams: " + ssc.inputStreams.size) // Should be 1

    // Create an EM context that incorporates the prior
    // Ugly workarounds
    val bc_em_config = ssc.sparkContext.broadcast(em_config)

    // Local mutable variables (bookkeeping and logging only)
    var current_step = 0

    // **** Asynchronous stream computations start here ****

    // Create samples
    //    val em_complete_samples = data_stream.flatMap({
    //      case (p_ob, w) =>
    //        val state = DistributedMutableStorage.state
    //        val t_before = System.currentTimeMillis
    //        val complete_samples = GammaSample.getSamples(p_ob, state, bc_em_config.value.num_em_samples)
    //        val t_after = System.currentTimeMillis
    //        println("Observation length is %d, time taken is %d " format (p_ob.mask.nodes.size, (t_after - t_before)))
    //        complete_samples.map((_, w))
    //    })
    
    for (rdd <- data_stream) {
      val glom_count = rdd.glom.map(_.size).collect
      val number_elts = rdd.count
      logInfo("Step %d : Current number of elements: %d" format (current_step, number_elts))
      logInfo("Step %d : GLOM Count: ".format(current_step) + glom_count.mkString(", "))
      current_step += 1
    }
    // Start computations forever
    ssc.start()
  }

  /**
   * Runs the algorithm.
   * @param em_config: the configuration set
   * @param extended_output: if true, will compute the statistics and write states to disk.
   */
  def startComputations(em_config: EMConfig, extended_output: Boolean): Unit = {
    // Creates the spark context
    val master_url:String = em_config.spark.master_url
    val framework:String = em_config.spark.framework
    val dt:Duration = em_config.data_description.streaming_chunk_length
    val ssc = new StreamingContext(master_url, framework, dt)

    val start_data_slice_index = ExperimentFunctions.startDataSliceIndex(em_config)

    // Build the stream
    // Watch out: since we are calling historical data on a real-time system, we need
    // to shift the clocks.
    val data_stream = ExperimentFunctions.createDStreamFromConfig(ssc, em_config)
//    logInfo("number of streams: " + ssc.inputStreams.size) // Should be 1

    val prior: Map[NodeId, GammaState] = ExperimentFunctions.createPriorFromConfig(em_config)

    // Create an EM context that incorporates the prior
    // Ugly workarounds
    val bc_em_config = ssc.sparkContext.broadcast(em_config)
    val bc_prior = {
      val dct = collection.mutable.Map(prior.toSeq: _*)
      ssc.sparkContext.broadcast(dct)
    }
    //    val em_context = new EMContext(em_config, prior)
    //    val bc_em_context = ssc.sc.broadcast(em_context)
    // Spark does not like immutable dictionaries. Grrrrrrrr.

    // Using the prior as an initial state.
    // Make sure to call it beforehand to initialize
    BroacastingUpdate.broadcastState(prior, ssc)

    // Local mutable variables (bookkeeping and logging only)
    var current_slice_index = start_data_slice_index
    var current_step = 0

    // **** Asynchronous stream computations start here ****

    // Create samples
    val em_complete_samples = data_stream.flatMap({
      case (p_ob, w) =>
        val state = DistributedMutableStorage.state
        val t_before = System.currentTimeMillis
        val complete_samples = GammaSample.getSamples(p_ob, state, bc_em_config.value.num_em_samples)
        val t_after = System.currentTimeMillis
        //        println("Observation length is %d, time taken is %d " format (p_ob.mask.nodes.size, (t_after - t_before)))
        complete_samples.map((_, w))
    })

    // Shuffling
    // First double is tt, second double is weight
    val samples_by_node = {
      val data = em_complete_samples
        .flatMap({
          case (cs, w) =>
            (cs.mask.nodes zip cs.partial).map(z => (z._1, (z._2, w)))
        })
      em_config.spark.num_reducers match {
        case Some(num_reducers) => {
          logInfo("Using custom number of reducers")
          data.groupByKey(num_reducers)
        }
        case None => data.groupByKey
      }
    }
    // Computing the new distribution
    // Warning! The order of the tts and the ws is different in this portion of the code
    // Is there a way to tag the types? 
    val new_node_states = samples_by_node.map(z => {
      val (nid, values) = z
      val prior_strength = bc_em_config.value.smoothing_strength
      val prior = bc_prior.value(nid)
      val tts = values.map(_._1).toArray
      val ws = values.map(_._2).toArray
      // New procedure seems to work?
      val new_state = GammaLearn.mle2(tts, ws, prior, prior_strength)
      // val new_state = GammaLearn.mle(tts, ws, prior, prior_strength)
      (nid, new_state)
    })

    // We need to update here the state to the nodes
    // Collect here the output and send it back to the node
    // TODO(tjh) is it the best way to do it??
    for (state_rdd <- new_node_states) {
      logInfo("Performing collect")
      val partial_state = state_rdd.collect.toMap
      // Make sure to fill in here
      val previous_state = DistributedMutableStorage.state
      val complete_new_state = partial_state ++ previous_state.toSeq.filterNot(z => partial_state.contains(z._1))
      // Send the new state to the other nodes
      // Broadcast disabled for the streaming experiments
      // It seems to change the computation time for the samples.
      //      if (BroacastingUpdate.broadcast_f != null) {
      //        logInfo("Waiting for previous broadcast to finish...")
      //        BroacastingUpdate.broadcast_f.apply()
      //        logInfo("Previous broadcast done")
      //      }
      //      BroacastingUpdate.broadcast_f = Futures.future({
      //        logInfo("About to start broadcasting state...")
      //        SparkEnv.set(ssc.sc.env)
      //        BroacastingUpdate.broadcastState(complete_new_state, ssc)
      //        logInfo("Broadcasting state finished")
      //      })

      // Store the output locally as well
      // This is blocking and might take longer than the slice time, not sure how it works out then...
      // TODO(tjh) what happens on D-streams with blocking IO??
      // TODO(?) would be nice to make a joint with the other stats
      // or create a new Dstream that contains the following partial stats and
      // gets joined with the rest of the stats
      if (extended_output) {
        logInfo("Writing state to disk")
        val stats = new StepStatistics
        stats.slice_index = current_slice_index
        stats.step = current_step
        stats.state = partial_state
        stats.full_state = complete_new_state
        InputOutput.saveStats(em_config, stats, null)
        current_step += 1
        logInfo("State written")
      }
    }

    if (extended_output) {
      // Compute the marginal log likelihood
      val marginal_ll = data_stream.map({
        case (p_ob, w) =>
          val state = DistributedMutableStorage.state
          (w, w * LearnSpark.marginalLogLikelihood(p_ob, state))
      })
        .reduce(mean_reducer)
        .map({
          case (all_weights, all_marginal_lls) =>
            all_marginal_lls / all_weights
        })

      // Compute ECLL before learning
      val weighted_ec_lls = em_complete_samples.map({
        case (obs, w) =>
          val state = DistributedMutableStorage.state
          (w, w * LearnSpark.completeLogLikelihood(obs, state))
      }).reduce(mean_reducer).map({ case (all_weights, all_ec_lls) => all_ec_lls / all_weights })

      // Compute the ECLL post learning
      // It should have improved
      val weighted_post_ec_lls = em_complete_samples.map({
        case (obs, w) =>
          // I am not sure here if it will distribute the new state fast enough to work
          val complete_new_state = DistributedMutableStorage.state
          (w, w * LearnSpark.completeLogLikelihood(obs, complete_new_state))
      }).reduce(mean_reducer).map({ case (all_weights, all_ec_lls) => all_ec_lls / all_weights })

      //    // TODO(?) Does it work?
      //    // No, it does not...
      //    for (
      //      number_elts <- data_stream.count;
      //      current_marginal_ll <- marginal_ll;
      //      current_weighted_ecll <- weighted_ec_lls;
      //      current_weighted_post_ecll <- weighted_post_ec_lls
      //    ) {
      //      logInfo("Current number of elements: %d" format number_elts)
      //      logInfo("Current marginal ll: %f" format current_marginal_ll)
      //      logInfo("Current ECLL: %f" format current_weighted_post_ecll)
      //      logInfo("Current post-learning ECLL: %f" format current_weighted_ecll)
      //    }

      // Workaround for now
      for (rdd <- data_stream)  {
        val number_elts = rdd.collect.head
        logInfo("Current number of elements: %d" format number_elts)
      }
      //       for (number_elts <- data_stream.count) {
      //         logInfo("Current number of elements: %d" format number_elts)
      //       }
      
      for (rdd <- em_complete_samples.count) {
        val number_elts = rdd.collect.head
        logInfo("Current number of samples: %d" format number_elts)
      }
      
      for (rdd <- marginal_ll) {
        val current_marginal_ll = rdd.collect.head
        logInfo("Current marginal ll: %f" format current_marginal_ll)
      }
      for (rdd <- weighted_ec_lls) {
        val current_weighted_ecll = rdd.collect.head
        logInfo("Current ECLL: %f" format current_weighted_ecll)
      }
      for (rdd <- weighted_post_ec_lls) {
        val current_weighted_post_ecll = rdd.collect.head
        logInfo("Current post-learning ECLL: %f" format current_weighted_post_ecll)
      }

    }

    // Start computations forever
    ssc.start()
  }

  /**
   * Runs the algorithm.
   * @param em_config: the configuration set
   * @param extended_output: if true, will compute the statistics and write states to disk.
   */
  def runBatchComputations(em_config: EMConfig, extended_output: Boolean): Unit = {
    val config = em_config.data_description
    // Creates the spark context
    val sc = new SparkContext(em_config.spark.master_url, em_config.spark.framework)

    val start_data_slice_index = ExperimentFunctions.startDataSliceIndex(em_config)

    // Build source of data (a simple stream, not a DStream)
    val data_stream = {
      val all_indexes = DataSliceIndex.list(config.source_name,
        config.network_id, net_type = config.network_type)
        .sorted
      val head_index = all_indexes.head
      val decay:SDuration = em_config.data_description.get.slice_decay_half_time
      val window_duration:SDuration = em_config.data_description.get.slice_window_duration
      // Precomputing all the RDDs
      val num_splits = 1
      val all_indexed_rdds = ObservationDStream.preloadRDDs(sc, all_indexes, num_splits) // Put some replication factor
      val slide_duration = em_config.data_description.streaming_chunk_length.getOrElse({ throw new Exception("should be set!"); Seconds(1) })
      new BatchStream(
        head_index, sc,
        window_duration, decay, all_indexed_rdds)
    }

    val prior: Map[NodeId, GammaState] = ExperimentFunctions.createPriorFromConfig(em_config)

    // Create an EM context that incorporates the prior
    // Ugly workarounds
    val bc_em_config = sc.broadcast(em_config)
    val bc_prior = {
      val dct = collection.mutable.Map(prior.toSeq: _*)
      sc.broadcast(dct)
    }
    val em_context = new EMContext(em_config, prior)
    val bc_em_context = sc.broadcast(em_context)
    // Local mutable variables (bookkeeping and logging only)
    var current_slice_index = start_data_slice_index
    var current_state = prior
    val start_time = {
      val start_time_index = ExperimentFunctions.startDataSliceIndex(em_config)
      val start_time = start_time_index.startTime.toDateTime().toDateMidnight()
      val dt3 = config.flatMap(_.start_time).map(_.getMillisOfDay()).getOrElse(0)
      new Time(start_time.getMillis() + dt3)
    }
    val day_weighting = config.day_exponential_weighting.value
    for (current_step <- 0 until em_config.data_description.num_steps) {
      val current_time = start_time + em_config.data_description.streaming_chunk_length.get * current_step
      val current_dtime = new DateTime(current_time.milliseconds)
      // Look for the step to begin with
      val start_step = {
        var iter_ = 0
        breakable {
          for (iter <- 0 until em_config.num_iterations) {
            // Reload the state from disk if it has already been computed.
            val fname = InputOutput.partialStateFilename(em_config, current_dtime, current_slice_index, iter)
            val f = new File(fname)
            if (f.exists()) {
              iter_ = iter + 1
              logInfo("Reloading state from " + fname)
              val new_state = InputOutput.readStates(fname)
              current_state = current_state.map({ case (k, v) => (k, new_state.getOrElse(k, v)) }).toSeq.toMap
            } else {
              logInfo("File does not exist: " + fname)
              logInfo("Starting computations after index " + iter_)
              break
            }
          }
        }
        iter_
      }
      logInfo("start_step is " + start_step)
      if (start_step < em_config.num_iterations) {
        // Create a RDD with all data required.
        // Get the RDD for each days, and weight them.
        val complete_data = (0 until config.get.day_window_size).flatMap(day_idx => {
          val data_time = current_dtime.minusDays(7 * day_idx)
          logInfo("Looking for data at time " + data_time)
          val data_sctime = Time(data_time.getMillis())
          val data = data_stream.compute(data_sctime)
          val weighted_data = data.map(_.map({ case (obs, w) => (w * day_weighting, obs) }))
          val count = weighted_data.count()
          logInfo("RDD for time %s has %d elements" format (data_time.toString(), count))
          weighted_data
        }).reduce(_ ++ _)

        val count = complete_data.count()
        logInfo("Got a complete RDD with %d elements " format count)
        // Did we get any data?
        if (count > 0) {
          val p_obs = complete_data
          val dtime = new DateTime(current_time.milliseconds)
          for (iter <- 0 until em_config.num_iterations) {
            logInfo("Processing iteration %d step %d : start time is %s" format (current_step, iter, (dtime)))
            val (new_state, complete_new_state, stats) = LearnSpark.learnWeightedStepSpark(
              p_obs,
              current_state,
              em_context,
              start_data_slice_index,
              iter,
              sc)

            if (extended_output) {
              logInfo("Writing state to disk")
              stats.slice_index = current_slice_index
              stats.step = iter
              stats.state = new_state
              stats.full_state = complete_new_state
              InputOutput.saveStats(em_config, stats, dtime)
              logInfo("State written")
            }
            current_state = complete_new_state
          }
        }
      }
    }
  }
}

object BroacastingUpdate extends MMLogging {

  var broadcast_f: Future[Unit] = null

  def broadcastState(state: Map[NodeId, GammaState], ssc: StreamingContext): Unit = {
    // Make sure we keep a local copy as well:
    DistributedMutableStorage.state = state
    // Spark does not like immutable hashmaps
    val wired_state = state.toSeq.toArray
    val n = 3 * Environment.numberNodes.getOrElse(10)
    logInfo("Broadcasting new state %d times..." format n)
    val bc_wired_state = ssc.sparkContext.broadcast(wired_state)
    // Create an RDD big enough so that every partition will be on a compute node
    val dummy = ssc.sparkContext.parallelize(1 to n, n)
    // Use the dummy RDD to traverse every node and update the state using the broadcast
    for (_ <- dummy) {
      // Will refer to the node's local singleton
      DistributedMutableStorage.state = Map.empty ++ bc_wired_state.value
    }
    logInfo("Broadcasting done")
  }
}

/**
 * A singleton placeholder for distributed mutable variables.
 */
object DistributedMutableStorage extends MMLogging {

  @transient private[this] var _state: Map[NodeId, GammaState] = null

  def state = {
    val s = this.synchronized(_state)
    if (s == null) {
      logError("****** THE STATE VARIABLE HAS NOT BEEN INITIALIZED ******")
    }
    s
  }

  def state_=(s: Map[NodeId, GammaState]): Unit = {
    this.synchronized {
      _state = s
    }
    logInfo("Updated state")
  }
}

