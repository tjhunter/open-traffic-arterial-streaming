package arterial_research.socc.experiments
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.SparkConfig
import core_extensions.MMLogging
import arterial_research.socc.Smoothing
import arterial_research.socc.io.DataSliceIndex
import arterial_research.socc.InputOutput
import arterial_research.socc.LearnSpark
import arterial_research.socc.EMContext
import spark.streaming.Seconds
import spark.streaming.Minutes
import org.joda.time.LocalTime
import arterial_research.socc.streaming.Algorithm

object Sliding1 extends App with MMLogging {

  val config = new DataDescription {
    network_id = 108
    network_type = "arterial"
    source_name = "cabspotting"
    date_range = "2010-02-2:2010-2-2"
    start_time = new LocalTime(18, 30, 0)
    num_steps = 50
    slice_window_duration = Minutes(60)
    slice_decay_half_time = Minutes(10*60)
    day_window_size = 2
    day_exponential_weighting = 0.99
    streaming_chunk_length = Minutes(20)
  }

  val em_config = new EMConfig {
    data_description = config
    num_iterations = 10
    num_em_samples = 100
    burn_in = 0
    save_states_dir = "/tmp/streaming1/"
//    save_full_states_dir = "/tmp/streaming1/"
    save_stats_dir = "/tmp/streaming1/"

    spark = new SparkConfig {
      master_url = "local[2]"
      framework = "open-traffic-arterial-spark"
    }
    smoothing_strength = 1.0
  }

  // We are going to run the streaming algorithm in batch mode
  // We will schedule a streaming call at some fixed intervals
  // Each interval will be decided beforehand.

  Algorithm.runBatchComputations(em_config, true)

}