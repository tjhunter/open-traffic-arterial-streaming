package arterial_research.socc
import collection.mutable.HashSet
import org.apache.commons.math.random.{ RandomGenerator, MersenneTwister }
import scala.collection.mutable.ArrayBuffer
import gamma.GammaSample
import arterial_research.socc.gamma.GammaState
import org.apache.commons.math.random.RandomData
import org.apache.commons.math.random.RandomDataImpl

object Sampler {

  private[this] val int_gen: RandomGenerator = new MersenneTwister()

  def sampleMask(num_nodes: Int, mask_size: Int): Array[NodeId] = {
    val res = new Array[NodeId](mask_size)
    val s = HashSet.empty[NodeId]
    while (s.size < mask_size) {
      val i: NodeId = int_gen.nextInt(num_nodes)
      if (!s.contains(i)) {
        res(s.size) = i
        s add i
      }
    }
    res
  }

  def getSyntheticSample(
    obs_mask: ObservationMask,
    state: Map[NodeId, GammaState]): Array[Value] = {
    obs_mask.nodes.map(nodeId => {
      val ns = state(nodeId)
      GammaSample.sampleState(ns)
    }).toArray
  }

  def generateSyntheticSamples(num_samples: Int, mask_size: Int,
    state: Map[NodeId, GammaState]): Seq[CompleteObservation] = {
    val num_nodes = state.size
    (0 until num_samples).map(i => {
      val mask = new ObservationMask(sampleMask(num_nodes, mask_size))
      val sample = getSyntheticSample(mask, state)
      new CompleteObservation(mask, sample)
    }).toSeq
  }

  val rand: RandomData = new RandomDataImpl(int_gen)

  def generateScaledSyntheticSamples(
    num_samples: Int,
    mask_size: Int,
    state: Map[NodeId, GammaState],
    min_scaling_factor: Double = 1.0,
    max_scaling_factor: Double = 1.0): Seq[CompleteObservation] = {
    require(min_scaling_factor >= 1e-1)
    require(max_scaling_factor >= 1e-1)
    require(min_scaling_factor <= max_scaling_factor)
    val num_nodes = state.size
    (0 until num_samples).map(i => {
      val mask_nodes = sampleMask(num_nodes, mask_size)
      val mask_weights = if (min_scaling_factor < max_scaling_factor) {
        mask_nodes.map(i => rand.nextUniform(min_scaling_factor, max_scaling_factor))
      } else {
        Array.fill(mask_nodes.size)(min_scaling_factor)
      }
      val mask = new ObservationMask(mask_nodes, mask_weights)
      val sample = getSyntheticSample(mask, state)
      val rescaled_sample = (mask_weights zip sample).map(z=>z._2 * z._1)
      new CompleteObservation(mask, rescaled_sample)
    }).toSeq
  }

}
