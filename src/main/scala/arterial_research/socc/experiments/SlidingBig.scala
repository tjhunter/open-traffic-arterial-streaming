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
import arterial_research.socc.TestConfig
import arterial_research.socc.streaming.ExperimentFunctions
import network.arterial1.ArterialLink
import arterial_research.socc.testing.GammaTTProvider
import arterial_research.socc.testing.ArterialGammaTTProvider
import org.scala_tools.time.Imports._
import scala.collection.mutable.ArrayBuffer
import netconfig.io.files.TrajectoryViterbi
import netconfig.io.json.JSonSerializer
import core.TimeUtils
import collection.JavaConversions._
import travel_time.TTProvider

trait SlidingBigSetup {
  val base_name = "/scratch/tjhunter_tmp_watson/logs/"
  
  val config = new DataDescription {
    network_id = 108
    network_type = "arterial"
    source_name = "cabspotting"
    date_range = "2010-10-12:2010-10-12"
    start_time = new LocalTime(6, 00, 0)
    num_steps = 10
    streaming_chunk_length = Minutes(20)

    slice_window_duration = Minutes(3 * 60)
    slice_decay_half_time = Minutes(60)
    day_window_size = 80
    day_exponential_weighting = 0.99
  }

  val em_config = new EMConfig {
    data_description = config
    num_iterations = 5
    num_em_samples = 100
    burn_in = 0
    save_states_dir = base_name + "sliding_big"
    save_full_states_dir = base_name + "sliding_big"
    save_stats_dir = base_name + "sliding_big"

    spark = new SparkConfig {
      master_url = "local[20]"
      framework = "open-traffic-arterial-spark"
    }
    smoothing_strength = 3.0
  }

  val test_config = new TestConfig {

  }
}

object SlidingBig extends App with MMLogging with SlidingBigSetup {
  // We are going to run the streaming algorithm in batch mode
  // We will schedule a streaming call at some fixed intervals
  // Each interval will be decided beforehand.
  Algorithm.runBatchComputations(em_config, true)
}
