package arterial_research.socc

import com.twitter.util.Config
import org.codehaus.jackson.JsonNode
import scalax.io.Resource
import spark.SparkContext
import core_extensions.MMLogging
import spark.RDD
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.io.DataFlow
import spark.streaming.{Duration=>SDuration}
import org.joda.time.LocalTime
import org.joda.time.Duration
import org.joda.time.DateTime

/**
 * Configuration for the spark job.
 */
class SparkConfig extends Config[SparkContext] with Serializable {
  var master_url = required[String]
  var home = optional[String]
  var jars = optional[Seq[String]]
  var framework = required[String]
  
  var num_reducers = optional[Int]
  
  
  def apply() = {
    val jars = sys.props.getOrElse("spark.jars", "").split(":")
    val home = sys.props.getOrElse("spark.home", "")
    new SparkContext(master_url, framework, home, jars)
  }
}

class EMContext(
    val config:EMConfig,
    val prior:Map[NodeId, GammaState]) extends Serializable {
}

/**
 * Configuration of the EM job.
 */
class EMConfig extends Config[EMContext] with Serializable {
  var num_iterations = required[Int]
  var num_em_samples = required[Int]
  var burn_in = required[Int]

  // Spark-specific options
  var spark = required[SparkConfig]
//  var num_splits = required[Int]
  
  /**
   * The portion of the data files that will be loaded from HDFS onto spark.
   * Default value is 1.0 (everything used).
   */
  var stream_initial_fraction = optional[Double]
  
  /**
   * Number of splits when preloading the HDFS data
   */
  var initial_replication_factor = optional[Int]

  // Logging-specific options
  var save_states_dir = optional[String]
  var save_full_states_dir = optional[String]
  var save_stats_dir = optional[String]
  
  // Initialization
//  var network = optional[Int]
//  var start_source = optional[String]
  
  /**
   * The data flow being used to run the algorithm.
   */
  var data_description = optional[DataDescription]
  
  // source of observations
  // Set it to "synthetic" to generate some fake observations.
  var source_url = required[String]
  /**
   * Description of the road network to get the link Ids.
   * Mandatory when using some observations.
   */
  var network_description_url = optional[String]

  /**
   * Number of links in the synthetic network.
   */
  var num_links = optional[Int]
  /**
   * Number of observations to generate for synthetic observations.
   */
  var num_observations = optional[Int]
  /**
   * Number of links in the observations (synthetic observations).
   */
  var num_links_in_observations = optional[Int]

  /**
   * The number of virtual observations for the smoother.
   * The values themselves are based on the speed limit of the network.
   */
  var smoothing_strength = required[Double](0.0)

  def apply() = {
    throw new NotImplementedException
  }
}


class DataDescription extends Config[DataFlow] with Serializable {
  // Parameters describing the source of data
  var network_id = required[Int]
  var network_type = required[String]
  var source_name = required[String]
  
  // Parameters for the source of data

  /**
   * The date range (in string format) over which the model will be run.
   */
  var date_range = required[String]
  
  /**
   * The start time in HH:MM:SS in the first date.
   */
  var start_time = optional[LocalTime]
  
  /**
   * The number of data slices that will be processed, starting from midnight of the 
   * first day.
   */
  var num_steps = required[Int]
  
  // Weighting parameters for the training.
  /**
   * The duration of the window considered inside each day.
   * Limited to be less than 10 minutes for now.
   */
  var slice_window_duration = required[SDuration]
  /**
   * Controls the exponential decay for the times inside a slice.
   */
  var slice_decay_half_time = required[SDuration]
  /**
   * The maximum number of days to look back.
   * The days that have no data will be skipped. 
   */
  var day_window_size = required[Int]
  /**
   * The decay law on the weights.
   * Will be multiplied with the slice exponential weighting.
   */
  var day_exponential_weighting = required[Double]
  
  /**
   * The time size of an RDD for streaming spark.
   * Assumed to be much less than 10 minutes: the code right now 
   * does not handle concatenating from different indexes.
   * you should set it to at most 20 seconds, 2-5 seconds are recommended.
   */
  var streaming_chunk_length = optional[SDuration]
  
  def apply() = {
     DataFlow.createDataFlowFromConfig(this)
  }
}

class TestConfig extends Config[Any] {
  var start_time = required[DateTime]
  var end_time = required[DateTime]
  var slice_window_duration = required[Duration]
  var states_directory = required[String]
  var test_output_directory = required[String]
  var network_id = required[Int]
  var network_type = required[String]
  var source_name = required[String]
  var em_iter = optional[Int]
  
  var split_intervals = required[Seq[(Duration,Duration)]]
  
  def apply() = {
    assert(false)
  }
}