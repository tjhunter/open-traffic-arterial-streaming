package arterial_research.socc.streaming

import arterial_research.socc.DataDescription
import arterial_research.socc.EMConfig
import arterial_research.socc.SparkConfig
import core_extensions.MMLogging
import arterial_research.socc.Smoothing
import arterial_research.socc.io.DataSliceIndex
import arterial_research.socc.InputOutput
import arterial_research.socc.LearnSpark
import arterial_research.socc.EMContext
import spark.streaming.StreamingContext
import spark.streaming.DStream
import spark.streaming.StreamingContext._
import arterial_research.socc.PartialObservation
import arterial_research.socc.gamma.GammaSample
import netconfig.io.Dates
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.NodeId
import org.joda.time.DateTime
import org.joda.time.Duration
import spark.streaming.Seconds
import core.storage.TimeRepr

object ExperimentFunctions extends MMLogging {

  def startDataSliceIndex(em_config: EMConfig): DataSliceIndex = {
    val data_conf = em_config.data_description.get
    import data_conf._
    val date = Dates.parseRange(em_config.data_description.date_range).get.head
    DataSliceIndex(source_name, network_id, date, network_type, 0)
  }

  def debugDStreamFromConfig(ssc: StreamingContext, em_config: EMConfig): DStream[(PartialObservation,Double)] = {
    val config = em_config.data_description
    val start_data_slice_index = startDataSliceIndex(em_config)
    // The first slice index in the dataset
    val head_index = DataSliceIndex.list(config.source_name,
      config.network_id, net_type = config.network_type)
      .sorted.head
    val slide_duration = em_config.data_description.streaming_chunk_length.getOrElse({ throw new Exception("should be set!"); Seconds(1) })
    val decay = em_config.data_description.get.slice_decay_half_time
    val window_duration = em_config.data_description.get.slice_window_duration
    // The complete stream over the dataset
    val data_stream = new ObservationDStream(head_index, ssc, slide_duration,
        window_duration, decay)
    val dt = ObservationDStream.indexStartTime(start_data_slice_index, head_index)
    // Compute the time between the head index start time and now
    val dt2 = {
      val now = new DateTime
      val start_slice_time = head_index.startTime
      (new Duration(now, start_slice_time.toDateTime)).getMillis()
    }
    logInfo("dt = " + dt)
    logInfo("dt2 = " + dt2)
    // The stream that provides real-time data
    val master_stream = new ShiftedInputDStream(data_stream, dt + dt2)
    // TODO(tjh) is it here to register?
    // TODO(tjh) this is very confusing, I need to make sure to register the *shifted* input stream
    ssc.registerInputStream(master_stream)
    master_stream
  }
  

  def createDStreamFromConfig(ssc: StreamingContext, em_config: EMConfig): DStream[(PartialObservation, Double)] = {
    val config = em_config.data_description
    // The first slice index in the dataset
    val all_indexes = DataSliceIndex.list(config.source_name,
      config.network_id, net_type = config.network_type)
      .sorted
    val head_index = all_indexes.head
    val dt = ObservationDStream.indexStartTime(startDataSliceIndex(em_config), head_index)
    // Compute the time between the head index start time and now
    val dt2 = {
      val now = new DateTime
      val start_slice_time = head_index.startTime
      (new Duration(now, start_slice_time.toDateTime)).getMillis()
    }
    // The time between the start of day and the date provided (if any)
    val dt3 = em_config.data_description match {
      case Some(data_description) => {
        data_description.start_time match {
          case Some(t) => {
            t.getMillisOfDay()
          }
          case None => 0
        }
      }
      case None => 0
    }
    logInfo("Local shift dt = " + dt)
    logInfo("Real-time correction shift: dt2 = " + dt2)
    logInfo("Day shift: dt3 = " + dt3)
    val decay = em_config.data_description.get.slice_decay_half_time
    val window_duration = em_config.data_description.get.slice_window_duration
    val initial_replication_factor = em_config.initial_replication_factor.getOrElse(3)
    logInfo("Initial replication factor: %d" format initial_replication_factor)
    // Precomputing all the RDDs
    val stream_initial_fraction = em_config.stream_initial_fraction.getOrElse(1.0)
    logInfo("Stream initial fraction: %f" format stream_initial_fraction)
//    val all_indexed_rdds = ObservationDStream.preloadRDDs(ssc.sc, all_indexes, 5) // Put some replication factor
    val all_indexed_rdds = ObservationDStream.preloadDistributedRDDs(ssc.sc, all_indexes, initial_replication_factor, stream_initial_fraction) // Put some replication factor
    val slide_duration = em_config.data_description.streaming_chunk_length.getOrElse({ throw new Exception("should be set!"); Seconds(1) })
    // The complete stream over the dataset, *starting from now in system clock*.
    val data_stream = new ObservationDStream(
        head_index, ssc, slide_duration,
        window_duration, decay, all_indexed_rdds)
    // The stream that provides historical streaming data
    val master_stream = new ShiftedInputDStream(data_stream, dt + dt2 + dt3)
    // TODO(tjh) is it here to register?
    // TODO(tjh) this is very confusing, I need to make sure to register the *shifted* input stream
    ssc.registerInputStream(master_stream)
    // The weights on the data
    val day_weights = (0 until config.day_window_size).map(day => math.pow(config.day_exponential_weighting, day)).toArray
    ObservationDStream.assembleStream(day_weights, master_stream)
  }

  def createPriorFromConfig(em_config: EMConfig): Map[NodeId, GammaState] = {
    logInfo("Creating prior...")
    val prior = Smoothing.createFromNetwork(em_config.data_description.network_id, em_config.data_description.network_type)
    logInfo("Creating prior done.")

    // Store the prior for some posterior control about the quality
    for (save_dir <- em_config.save_states_dir) {
    logInfo("Saving prior...")
      val fname = "%s/%s-%s.txt" format (save_dir, "PRIOR", "PRIOR")
      InputOutput.writeState(fname, prior)
    logInfo("Saving prior done")
    }
    prior
  }
}