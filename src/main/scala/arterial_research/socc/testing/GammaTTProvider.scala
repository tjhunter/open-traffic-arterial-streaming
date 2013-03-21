package arterial_research.socc.testing

import network.arterial1.ArterialLink
import arterial_research.socc.gamma.GammaState
import org.joda.time.DateTime
import netconfig.Route
import arterial_research.socc.gamma.GammaMixture
import arterial_research.socc.NodeId
import netconfig.storage.Codec
import arterial_research.socc.io.ConvertTrajectories
import netconfig.NetconfigException
import arterial_research.socc.EMConfig
import arterial_research.socc.streaming.ExperimentFunctions
import org.scala_tools.time.Imports._
import arterial_research.socc.InputOutput
import java.io.File
import netconfig.io.files.SerializedNetwork
import netconfig.io.json.JSonSerializer
import core_extensions.MMLogging
import network.arterial1.NetworkBuilder
import netconfig.storage.LinkIDRepr
import travel_time._

class GammaTTDistribution(val mix: GammaMixture) extends ScalaTravelTimeDistribution {
  def mode = mix.mode
  def mean = mix.mean
  def variance = mix.variance
  def entropy = mix.entropy
  def cdf(x: Double) = mix.cdf(x)
  def logNormalizer = mix.logNormalizer
  def unnormalizedLogPdf(x: Double) = mix.unnormalizedLogPdf(x)
  def draw() = mix.draw()
}

class GammaTTProvider private[testing] (
  state: Map[NodeId, GammaState],
  node_map: Map[ArterialLink, NodeId],
  codec: Codec[ArterialLink]) extends ScalaTTProvider[ArterialLink] {

  private val lengths = node_map.keys.map(al => {
    val lid_r = al.id
    val l = al.length
    (lid_r, l)
  }).toMap

  private val node_map_repr = node_map.map({
    case (al, nid) =>
      (al.id, nid)
  }).toMap

  def travelTimeSafe(start_time: DateTime, route: Route[ArterialLink]): Either[ScalaTravelTimeDistribution, NetconfigException] =
    travelTimeSafe(route)

  def travelTimeSafe(route: Route[ArterialLink]): Either[ScalaTravelTimeDistribution, NetconfigException] = {
    // Convert the route to routeRepr
    val route_repr = codec.toRepr(route, false)
    // Get corresponding observation mask
    val mask_opt = ConvertTrajectories.convertRouteScaled(route_repr, lengths, node_map_repr)
    // Get the distribution
    mask_opt.map(mask => {
      val gss = mask.nodes.map(state.apply _).toArray
      // TODO(?) this cast is not pretty
      val dis = GammaMixture.positiveCombinationOfGammas(gss, mask.weights)
      val tt_dis = new GammaTTDistribution(dis)
      Left(tt_dis)
    }).getOrElse({
      val e = new NetconfigException(null, "Could not process route: " + route)
      Right(e)
    })
  }
}

object ArterialGammaTTProvider extends MMLogging {
  def loadArterialNetwork(network_id: Int, net_type: String): Map[LinkIDRepr, ArterialLink] = {
    logInfo("Loading network type=%s id=%d..." format (net_type, network_id))
    val fname = SerializedNetwork.fileName(network_id, net_type)
    val glrs = JSonSerializer.getGenericLinks(fname).toSeq
    val anet = NetworkBuilder.build(glrs).map(alink => (alink.id, alink)).toMap
    logInfo("Loading network done")
    anet
  }

  def createFromConfig(em_config: EMConfig, em_iter: Int, anet: Map[LinkIDRepr, ArterialLink]): ScalaTTProvider[ArterialLink] = {
    val config = em_config.data_description
    // List all the files
    val start_time_index = ExperimentFunctions.startDataSliceIndex(em_config)
    val start_time = {
      val start_time = start_time_index.startTime.toDateTime().toDateMidnight()
      val dt3 = config.flatMap(_.start_time).map(_.getMillisOfDay()).getOrElse(0).millis
      start_time.toDateTime() + dt3
    }

    val stream_chunk_length = em_config.data_description.streaming_chunk_length.get.milliseconds.toInt
    logInfo("stream_chunk_length: " + stream_chunk_length)

    val all_tts = (0 until em_config.data_description.num_steps).flatMap { current_step =>
      val current_dtime = start_time + (current_step * stream_chunk_length).millis
      val fname = InputOutput.partialStateFilename(em_config, current_dtime, start_time_index, em_iter)
      val f = new File(fname)
      if (f.exists) {
        logInfo("Registering state file " + fname)
        Some(current_dtime)
      } else {
        logInfo("Tried time %s and file %s".format(""+current_dtime, fname))
        None
        }
    }

    val save_dir = em_config.save_full_states_dir.get

    // Rebuild the 
    val full_state_fnames = Seq("%s/PRIOR-PRIOR.txt" format (save_dir)) ++ (for (i <- 0 until all_tts.size) yield {
      val current_dtime = all_tts(i)
      "%s/full-%s-%d.txt" format (save_dir, current_dtime.toString(), em_iter)
    })

    for (i <- 0 until all_tts.size) {
      val current_dtime = all_tts(i)
      val state_fname = "%s/full-%s-%d.txt" format (save_dir, current_dtime.toString(), em_iter)
      if (!(new File(state_fname)).exists()) {
        logInfo("Rebuilding full state " + state_fname)
        val previous_state_fname = full_state_fnames(i)
        val previous_state = InputOutput.readStates(previous_state_fname)
        logInfo("Read previous state: %s (%d elements)" format (previous_state_fname, previous_state.size))
        val fname = InputOutput.partialStateFilename(em_config, current_dtime, start_time_index, em_iter)
        val partial_state = InputOutput.readStates(fname)
        logInfo("Read partial state: %s (%d elements)" format (fname, partial_state.size))
        val full_state = previous_state ++ partial_state
        logInfo("Write full state: %s (%d elements)" format (state_fname, full_state.size))
        InputOutput.writeState(state_fname, full_state)
      }
    }

    // Load all the arterial data here
    val network_id: Int = config.network_id
    val net_type: String = config.network_type
    val codec = JSonSerializer.from(anet)
    val node_map = ConvertTrajectories.createArterialIndexMap(anet.values).map({ case (lidr, nid) => (anet(lidr), nid) })
    // Define the loading function

    def readSlice(dt: DateTime): ScalaTTProvider[ArterialLink] = {
      val tstring = dt.toString()
      val state_fname = "%s/full-%s-%d.txt" format (save_dir, tstring, em_iter)
      logInfo("Loading file " + state_fname)
      val state = InputOutput.readStates(state_fname)
      val ttp = new GammaTTProvider(state, node_map, codec)
      ttp
    }

    val interslice_max_duration = config.get.slice_window_duration.milliseconds.toInt.millis

    SliceTTProvider.apply(all_tts, readSlice, interslice_max_duration)
  }
}
