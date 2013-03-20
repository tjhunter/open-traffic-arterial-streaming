package arterial_research.socc.experiments
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
import arterial_research.socc.streaming.ExperimentFunctions
import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.EMContext
import arterial_research.socc.PartialObservation
import arterial_research.socc.SparkConfig
import spark.streaming.StreamingContext.toPairDStreamFunctions
import spark.streaming.DStream
import spark.streaming.Seconds
import spark.streaming.StreamingContext
import spark.Logging
import arterial_research.socc.Environment
import arterial_research.socc.streaming.Algorithm
import org.joda.time.LocalTime
import spark.streaming.Minutes

/**
 * This implementation tries to make full use of the DStream API.
 * TODO: spark cannot use the app trait because it then tries to embed the spark context if one uses logInfo
 */
trait Streaming1Config {

  def config = new DataDescription {
    network_id = 108
    network_type = "arterial"
    source_name = "cabspotting"
    date_range = "2010-02-19:2010-2-19"
    start_time = new LocalTime(11, 30, 0)
    num_steps = 100
    slice_window_duration = Minutes(21)
    slice_decay_half_time = Minutes(60)
    day_window_size = 2
    day_exponential_weighting = 0.9
    streaming_chunk_length = Seconds(5)
  }

  def em_config = new EMConfig {
    data_description = config
    num_iterations = 1
    num_em_samples = 100
    burn_in = 100
    //      save_states_dir = "/tmp/streaming1/"
    //      save_full_states_dir = "/tmp/streaming1/"
    //      save_stats_dir = "/tmp/streaming1/"

    spark = new SparkConfig {
      master_url = Environment.mesosHostname.get
      framework = "open-traffic-arterial-spark"
    }
//    num_splits = 8
    smoothing_strength = 3.0
  }
}

trait Streaming5 extends Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(0.02)
    c.initial_replication_factor = Some(5)
    c
  }
}

trait Streaming10 extends Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(0.02 * 2)
    c.initial_replication_factor = Some(6)
    c
  }
}

trait Streaming20 extends Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(0.02 * 4)
    c.initial_replication_factor = Some(12)
    c
  }
}

trait Streaming40 extends Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(0.02 * 8)
    c.spark.num_reducers = Some(80)
    c.initial_replication_factor = Some(200)
    c
  }
}

trait Streaming80 extends Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(0.02 * 16)
    c.spark.num_reducers = Some(160)
    c.initial_replication_factor = Some(300)
    c
  }
}

// Runnable algorithms

object Streaming1 extends App with Logging with Streaming1Config {
  Algorithm.startComputations(em_config, false)
}

object Streaming5A extends App with Logging with Streaming5 {
  Algorithm.startComputations(em_config, false)
}

object Streaming10A extends App with Logging with Streaming10 {
  Algorithm.startComputations(em_config, false)
}

object Streaming20A extends App with Logging with Streaming20 {
  Algorithm.startComputations(em_config, false)
}

object Streaming40A extends App with Logging with Streaming40 {
  Algorithm.startComputations(em_config, false)
}

object Streaming80A extends App with Logging with Streaming80 {
  Algorithm.startComputations(em_config, false)
}

// Counting runs
object Streaming5Complete extends App with Logging with Streaming1Config {
  override def em_config = {
    val c = super.em_config
    c.stream_initial_fraction = Some(1.0)
    c.initial_replication_factor = Some(5)
    c
  }
  Algorithm.countElements(em_config)
}


object Streaming5B extends App with Logging with Streaming5 {
  Algorithm.countElements(em_config)
}

object Streaming10B extends App with Logging with Streaming10 {
  Algorithm.countElements(em_config)
}

object Streaming20B extends App with Logging with Streaming20 {
  Algorithm.countElements(em_config)
}

object Streaming40B extends App with Logging with Streaming40 {
  Algorithm.countElements(em_config)
}
