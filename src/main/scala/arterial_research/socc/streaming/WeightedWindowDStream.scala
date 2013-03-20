package arterial_research.socc.streaming

import spark.streaming.DStream
import spark.streaming.Time
import spark.RDD
import spark.Logging
import scala.collection.mutable.Queue
import org.scala_tools.time.Imports._

import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of a weighted window algorithm using a FIFO queue.
 *
 * TODO(?) Should it be made a StatefulDStream??
 *
 * Tries to minimize the number of back calls to the parent D-stream.
 */
class WeightedWindowedDStream[T: ClassManifest, U: ClassManifest](
  @transient parent: DStream[U],
  weighingFunction: (RDD[U], Double) => RDD[T],
  weights: IndexedSeq[Double]) extends DStream[T](parent.ssc) with Logging {

  logInfo(this + " has parent "+parent)
  
  // The queue will contain the elements queried from the stream so far.
  // Ordered in decreasing order: oldest element on the top.
  @transient val queue = new Queue[(Time, RDD[U])]

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    logInfo(this + " Called with validTime = " + validTime + " <-> " + (new DateTime(validTime.milliseconds)))
    // We get the RDD corresponding to this time:
    // TODO(?) What is the difference with getOrCompute?
    val current_rdd = parent.compute(validTime)
    logInfo("Got an RDD: "+current_rdd)
    // Otherwise, push it to the queue
    for (rdd <- current_rdd) {
      queue += validTime -> rdd
    }
    // Drop the old elements from the queue
    val drop_time = validTime - slideTime * weights.size
    queue.dequeueAll(_._1 <= drop_time)
    assert(queue.size <= weights.size)
    // If the queue is empty, return None
    if (queue.isEmpty) {
      None
    } else {
      // Otherwise, return the union
      // Use the time indexes to make a joint
      // Requires the exact use of a sliding time multiple
      assert(queue.forall(z => (z._1.milliseconds - validTime.milliseconds) % slideTime.milliseconds == 0), queue)
      val mapped_rdds = queue.map({
        case (t, rdd) =>
          val i = ((validTime - t).milliseconds / slideTime.milliseconds).toInt
          assert(i >= 0, (i, queue))
          assert(i < queue.size)
          val w = weights(i)
          weighingFunction(rdd, w)
      })
      val all_elts = mapped_rdds.reduce(_ ++ _)
      Some(all_elts)
    }

    //    val parentRDDs = new ArrayBuffer[RDD[T]]()
    //    val windowEndTime = validTime
    //    val windowStartTime = if (allowPartialWindows && windowEndTime - windowTime < parent.getZeroTime) {
    //      parent.getZeroTime
    //    } else {
    //      windowEndTime - windowTime
    //    }
    //
    //    logInfo("Window = " + windowStartTime + " - " + windowEndTime)
    //    logInfo("Parent.zeroTime = " + parent.getZeroTime)
    //
    //    if (windowStartTime >= parent.getZeroTime) {
    //      // Walk back through time, from the 'windowEndTime' to 'windowStartTime'
    //      // and get all parent RDDs from the parent DStream
    //      var t = windowEndTime
    //      var i = 0
    //      while (t > windowStartTime) {
    //        parent.getOrCompute(t) match {
    //          case Some(rdd) => parentRDDs += weighingFunction(rdd, weights(i))
    //          case None => {
    //            // TODO(tjh) I am not going to throw an exception here
    ////            throw new Exception("Could not generate parent RDD for time " + t)
    //          }
    //        }
    //        t -= parent.slideTime
    //        i += 1
    //      }
    //    }
    //
    //    // Do a union of all parent RDDs to generate the new RDD
    //    if (parentRDDs.size > 0) {
    //      Some(new UnionRDD(ssc.sc, parentRDDs))
    //    } else {
    //      None
    //    }
  }
}



