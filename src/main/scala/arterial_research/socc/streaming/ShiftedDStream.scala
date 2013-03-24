package arterial_research.socc.streaming

import spark.streaming.DStream
import spark.streaming.dstream.InputDStream
import spark.streaming.Time
import spark.streaming.{Duration=>SDuration}
import org.joda.time.Duration
import org.joda.time.DateTime

/**
 * Shifts the time forward or backward.
 */
class ShiftedDStream[T: ClassManifest] (@transient parent:DStream[T], val shift:SDuration) extends DStream[T](parent.context) {

  def slideDuration = parent.slideDuration
  
  def compute(t:Time) = {
    val shifted_t = t + shift
    parent.compute(shifted_t)
//    if (shifted_t < parent.getZeroTime) {
//      // TODO(tjh) I am not sure about the semantic of None since it makes WindowedDStream crash
//      // I need here an empty RDD
//      None
//    } else {
//      parent.compute(shifted_t)
//    }
  }
  
  def dependencies = List(parent)
  
  def slideTime = parent.slideDuration
}

/**
 * Shifts the time forward for an input DStream.
 */
class ShiftedInputDStream[T: ClassManifest] (val parent:InputDStream[T], val shift:SDuration) extends InputDStream[T](parent.context) {
  logInfo("shift: %d millis (%s)" format (shift, new Duration(shift.milliseconds)))
  
  def compute(validTime:Time) = {
    val shifted_t = validTime + shift
    logInfo(this + " Called with validTime = " + validTime + " <-> " + (new DateTime(validTime.milliseconds)))
    parent.compute(shifted_t)
    // TODO(tjh): I would have assumed something like that:??
//    logInfo("zero  time: "+parent.getZeroTime)
//    if (shifted_t < parent.getZeroTime) {
//      // TODO(tjh) I am not sure about the semantic of None since it makes WindowedDStream crash
//      // I need here an empty RDD
//      None
//    } else {
//      parent.compute(shifted_t)
//    }
  }
  
  def start = parent.start()
  
  def stop = parent.stop()
  
  override def slideDuration = parent.slideDuration
}