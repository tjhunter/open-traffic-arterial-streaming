package arterial_research.socc.experiments
import core_extensions.MMLogging
import arterial_research.socc._
import spark.streaming.Seconds
import arterial_research.socc.streaming.Algorithm

/**
 * Simple experiment that replicates Streaming1
 */
object Streaming32A extends App with MMLogging {

  val em_config = new EMConfig {
    data_description = new DataDescription {
      network_id = 108
      network_type = "arterial"
      source_name = "cabspotting"
      date_range = "2010-02-9:2010-2-9"
      num_steps = 400
      slice_window_duration = Seconds(120)
      slice_decay_half_time = Seconds(60)
      day_window_size = 2
      day_exponential_weighting = 0.9
      streaming_chunk_length = Seconds(3)
    }
    
    num_iterations = 1
    num_em_samples = 1
    burn_in = 1
    save_states_dir = "/tmp/streaming1/"
    save_full_states_dir = "/tmp/streaming1/"
    save_stats_dir = "/tmp/streaming1/"

    spark = new SparkConfig {
      master_url = {
        val s: String = Environment.mesosHostname.getOrElse({ logError("You have to provide mesos hostname"); assert(false); "" })
        s
      }
      framework = "open-traffic-arterial-spark-streaming32a"
    }
//    num_splits = 8
    smoothing_strength = 3.0
  }
  
  Algorithm.startComputations(em_config, false)

}

/**
 * Much larger experiment that tries to load a a month of data.
 */
object Streaming32B extends App with MMLogging {

  val em_config = new EMConfig {
    data_description = new DataDescription {
      network_id = 108
      network_type = "arterial"
      source_name = "cabspotting"
      date_range = "2010-02-9:2010-2-9"
      num_steps = 400
      slice_window_duration = Seconds(120) // This makes 5 minutes, still very few
      slice_decay_half_time = Seconds(60)
      day_window_size = 10  // 10 days, we should increase to a month at least
      day_exponential_weighting = 0.9
      streaming_chunk_length = Seconds(3)
    }
    
    num_iterations = 1
    num_em_samples = 1
    burn_in = 1
    save_states_dir = "/tmp/streaming1/"
    save_full_states_dir = "/tmp/streaming1/"
    save_stats_dir = "/tmp/streaming1/"

    spark = new SparkConfig {
      master_url = {
        val s: String = Environment.mesosHostname.getOrElse({ logError("You have to provide mesos hostname"); assert(false); "" })
        s
      }
      framework = "open-traffic-arterial-spark-streaming32b"
    }
//    num_splits = 8
    smoothing_strength = 3.0
  }
  
  Algorithm.startComputations(em_config, false)

}