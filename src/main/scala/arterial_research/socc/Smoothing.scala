package arterial_research.socc
import netconfig.io.files.SerializedNetwork
import netconfig.io.json.JSonSerializer
import netconfig.storage.LinkIDReprOrdering
import netconfig.storage.LinkIDRepr
import arterial_research.socc.gamma.GammaState

/**
 * Some functions to define smoothing values over the learning parameters.
 */
object Smoothing {

  /**
   * Loads a network and gets some good start values for the gamma distribution.
   */
  def createFromNetwork(network_id:Int, net_type:String):Map[NodeId, GammaState] = {
    // Load the generic link representation
    val fname = SerializedNetwork.fileName(network_id, net_type)
    val glrs = JSonSerializer.getGenericLinks(fname)
    // Build the canonical map:
    val map:Map[LinkIDRepr, NodeId] = glrs.map(_.id).toSeq.sorted(LinkIDReprOrdering).zipWithIndex.toMap
    // Use 70% of the speed limit
    // Variance fixed to 60^2 seconds
    glrs.map(glr =>{
      val l = glr.length.get
      val speed = glr.speedLimit.get
      val mean = 0.7 * l / speed
      val variance = math.min(mean * mean, 60 * 60)
      (map(glr.id), GammaState.fromMeanVariance(mean, variance))
    }).toMap
  }
}