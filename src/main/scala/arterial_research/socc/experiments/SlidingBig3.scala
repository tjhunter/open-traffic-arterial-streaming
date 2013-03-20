package arterial_research.socc.experiments
import arterial_research.socc.streaming.Algorithm
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.SparkConfig
import arterial_research.socc.TestConfig
import core_extensions.MMLogging
import spark.streaming.Minutes

import org.joda.time.LocalTime
import arterial_research.socc.streaming.Algorithm
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.SparkConfig
import arterial_research.socc.TestConfig
import core_extensions.MMLogging
import spark.streaming.Minutes
import org.joda.time.DateTime
import org.scala_tools.time.Imports._

/**
 * Window much shorter.
 */
trait SlidingBig3Setup {
  val base_name = "/scratch/tjhunter_tmp_watson/logs/"
  val exp_name = "sliding_big_3"
  val test_base_name = "/windows/D/arterial_experiments/tase/"

  val config = new DataDescription {
    network_id = 108
    network_type = "arterial"
    source_name = "cabspotting"
    date_range = "2010-10-12:2010-10-12"
    start_time = new LocalTime(6, 00, 0)
    num_steps = 40
    streaming_chunk_length = Minutes(5)

    slice_window_duration = Minutes(3 * 60)
    slice_decay_half_time = Minutes(60)
    day_window_size = 80
    day_exponential_weighting = 0.99
  }

  val em_config = new EMConfig {
    data_description = config
    num_iterations = 1
    num_em_samples = 100
    burn_in = 0
    save_states_dir = base_name + exp_name
    save_full_states_dir = base_name + exp_name
    save_stats_dir = base_name + exp_name

    spark = new SparkConfig {
      master_url = "local[20]"
      framework = "open-traffic-arterial-spark"
    }
    smoothing_strength = 3.0
  }

  val test_config = new TestConfig {
    start_time = new DateTime(2010, 10, 12, 6, 0, 0)
    end_time = new DateTime(2010, 10, 12, 9, 0, 0)
    slice_window_duration = (config.slice_window_duration.value.milliseconds / 1000).toInt.seconds.toDuration
    states_directory = test_base_name + "/states/SlidingBig3/"
    test_output_directory = test_base_name + "/perf/SlidingBig3/"
    network_id = config.network_id
    network_type = config.network_type
    source_name = config.source_name
    em_iter = 0

    split_intervals = Seq(
      (50 seconds, 70 seconds),
      (3.minutes + 50.seconds, 3.minutes + 70.seconds),
      (10.minutes + 50.seconds, 10.minutes + 70.seconds),
      (20.minutes + 50.seconds, 20.minutes + 70.seconds)).map(z => (z._1.toDuration, z._2.toDuration))
  }
}

object SlidingBig3 extends App with MMLogging with SlidingBig3Setup {
  // We are going to run the streaming algorithm in batch mode
  // We will schedule a streaming call at some fixed intervals
  // Each interval will be decided beforehand.
  Algorithm.runBatchComputations(em_config, true)
}

object SlidingBig3Test extends App with MMLogging with SlidingBig3Setup {
  // We are going to run the streaming algorithm in batch mode
  // We will schedule a streaming call at some fixed intervals
  // Each interval will be decided beforehand.
  Test.runTests(em_config, test_config)
}
