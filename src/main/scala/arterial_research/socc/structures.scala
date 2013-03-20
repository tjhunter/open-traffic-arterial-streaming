package arterial_research.socc

import java.io.Serializable
import org.joda.time.LocalDateTime
import org.joda.time.DateTime
import arterial_research.socc.gamma.GammaState

case class ObservationMask(val nodes: Array[Int], val weights: Array[Double]) extends Serializable {
  def this(nodes: Array[Int]) = this(nodes, Array.fill(nodes.size)(1.0))
}

case class CompleteObservation(val mask: ObservationMask,
  val partial: Array[Value]) extends Serializable {
  def toPartial: PartialObservation = new PartialObservation(mask, partial.sum)
}

case class PartialObservation(
  val mask: ObservationMask,
  val total: Double) extends Serializable

case class Record(val time: DateTime, val obs: PartialObservation) {
  def toRepr = RecordRepr(time.toLocalDateTime(), obs)
}

case class RecordRepr(val time: LocalDateTime, val obs: PartialObservation) {
  def toRecord = Record(time.toDateTime, obs)
}

object ObservationMask {
  def apply(nodes: Array[Int]) = new ObservationMask(nodes, Array.fill(nodes.size)(1.0))
}

case class StateRecord(
    val nodeId:Int, 
    val state:GammaState) {
  def pair = (nodeId.asInstanceOf[NodeId], state)
}