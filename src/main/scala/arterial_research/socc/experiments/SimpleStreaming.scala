package arterial_research.socc.experiments
import com.twitter.util.Config.fromOption
import com.twitter.util.Config.fromRequired
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.NodeId
import arterial_research.socc.streaming.ExperimentFunctions
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.EMContext
import arterial_research.socc.InputOutput
import arterial_research.socc.LearnSpark
import arterial_research.socc.PartialObservation
import arterial_research.socc.SparkConfig
import core_extensions.MMLogging
import spark.streaming.DStream
import spark.streaming.Seconds
import spark.streaming.StreamingContext
import spark.streaming.Minutes


/**
 * Simple wrapper around the RDD code to stream data inside as RDDs
 */
object SimpleStreaming extends App with MMLogging {

  import LearnSpark._

  val config = new DataDescription {
    network_id = 108
    network_type = "arterial"
    source_name = "cabspotting"
    date_range = "2009-4-2:2009-5-8"
    num_steps = 400
    slice_window_duration = Minutes(3)
    slice_decay_half_time = Seconds(120)
    day_window_size = 2
    day_exponential_weighting = 0.1
    streaming_chunk_length = Seconds(3)
  }

  val em_config = new EMConfig {
    data_description = config
    num_iterations = 2
    num_em_samples = 40
    burn_in = 1000
    save_states_dir = "/tmp/simple1/"
    save_full_states_dir = "/tmp/simple1/"
    save_stats_dir = "/tmp/simpl1/"

    spark = new SparkConfig {
      master_url = "local[2]"
      framework = "open-traffic-arterial-spark"
    }
//    num_splits = 8
    smoothing_strength = 3.0
  }

  // Creates the spark context
  val ssc = new StreamingContext(em_config.spark.master_url, em_config.spark.framework)
  for (dt <- em_config.data_description.streaming_chunk_length) {
//    ssc.batchDuration = dt
  }

  val start_data_slice_index = ExperimentFunctions.startDataSliceIndex(em_config)

  // Build the streaming plan
  // Watch out: since we are calling historical data on a real-time system, we need
  // to shift the clocks.
  val data_stream: DStream[(PartialObservation, Double)] = ExperimentFunctions.createDStreamFromConfig(ssc, em_config)
//  logInfo("number of streams: " + ssc.inputStreams.size) // Should be 1

  val prior: Map[NodeId, GammaState] = ExperimentFunctions.createPriorFromConfig(em_config)
  // Create an EM context that incorporates the prior
  val em_context = new EMContext(em_config, prior)

  // Run the main loop on the execution plan.
  var state = prior
//  val num_splits = em_config.num_splits

  val bc_em_context = ssc.sc.broadcast(em_context)

  var num_steps = 0
  var current_slice_index = start_data_slice_index
  data_stream.foreachRDD(weighted_obs => {
    logInfo("Performing step %d" format (num_steps))
    val (new_state, stats) = LearnSpark.learnWeightedWithSparkStep(
      weighted_obs,
      state,
      em_context,
      start_data_slice_index,
      num_steps,
      ssc.sc)
    InputOutput.saveStats(em_config, stats, null)
    state = new_state
    num_steps += 1
    current_slice_index = current_slice_index.nextSlice
  })

  ssc.start()
}