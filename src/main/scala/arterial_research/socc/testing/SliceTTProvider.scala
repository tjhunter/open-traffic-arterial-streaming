package arterial_research.socc.testing
import scala.collection.SortedMap
import org.joda.time.DateTime
import netconfig.Link
import netconfig.NetconfigException
import netconfig.Route
import org.joda.time.Duration
import org.scala_tools.time.Imports._
import scala.collection.SortedSet
import scala.collection.mutable.WeakHashMap
import core_extensions.MMLogging
import travel_time._

/**
 * Provides travel times for different slices of time.
 */
class SliceTTProvider[L <: Link] private (
  slice_times: Seq[DateTime],
  data_gen: DateTime => ScalaTTProvider[L],
  inter_slice_duration: Duration) extends ScalaTTProvider[L] with MMLogging {

  private[this] val slices_cache = new WeakHashMap[DateTime, ScalaTTProvider[L]]

  def travelTimeSafe(start_time: DateTime, route: Route[L]): SafeReturnType = {
    // Find the first element in the map before the start time
    // See if the duration between the start time and the slice start time is small enough
    val time_before = slice_times
      .filter(_ <= start_time)
      .lastOption
      .toLeft({
        new NetconfigException(null, "Could not find slice before time " + start_time)
      }).left
      .flatMap(t => {
        val dt = (t to start_time).toDuration()
        if (dt > inter_slice_duration) {
          Right(new NetconfigException(null, "Closest slice is out of range"))
        } else {
          Left(t)
        }
      })
    time_before.left.flatMap(t => {
      val this_slice = slices_cache.getOrElseUpdate(t, {
        logInfo("Loading data for slice t = " + t)
        data_gen(t)
      })
      this_slice.travelTimeSafe(start_time, route)
    })
  }
}

object SliceTTProvider {
  def apply[L <: Link](slice_times: Seq[DateTime],
    data_gen: DateTime => ScalaTTProvider[L],
    inter_slice_duration: Duration): SliceTTProvider[L] = {
    val ordered_slice_times = slice_times.sortWith(_ <= _)
    new SliceTTProvider(ordered_slice_times, data_gen, inter_slice_duration)
  }
}