package arterial_research.socc.streaming

import spark.streaming.DStream
import scala.actors.Futures
import spark.SparkContext
import spark.storage.StorageLevel
import spark.streaming.dstream.InputDStream
import arterial_research.socc.io.DataSliceIndex
import spark.streaming.StreamingContext
import spark.streaming.Time
import spark.streaming.Seconds
import spark.streaming.Milliseconds
import spark.streaming.dstream.UnionDStream
import spark.RDD
import spark.Logging
import spark.HashPartitioner
import spark.SparkContext._
import spark.SparkEnv
import core_extensions.MMLogging
import arterial_research.socc.PartialObservation
import arterial_research.socc.InputOutput
import arterial_research.socc.Record
import scala.io.Source
import java.io.File
import org.joda.time.DateTime
import org.joda.time.Duration
import org.scala_tools.time.Imports._

trait Stream extends Logging {

  def window_length: Time
  def decay_half_time: Time
  def preloadedData: DataSliceIndex => Option[RDD[Record]]
  def context: SparkContext
  def start_index: DataSliceIndex

  lazy val emptyRDD = {
    val d = Seq.empty[(PartialObservation, Double)]
    val rdd = context.makeRDD(d, 1)
    rdd
  }

  def compute(valid_time: Time): Option[RDD[(PartialObservation, Double)]] = {
    // The corresponding local date for the start and the end of the slice
    val end_slice_date = (new DateTime(valid_time.milliseconds))
    val begin_slice_date = end_slice_date - ((window_length.milliseconds / 1000).toInt.seconds)
    logInfo("Called with validTime = " + valid_time + " <-> (%s, %s)".format(begin_slice_date, end_slice_date))
    // Find the index corresponding to the requested time
    val current_slice = {
      val start_day = start_index.copy(date = end_slice_date.toLocalDate(), slice = 0).startTime
      val d = new Duration(start_day.toDateTime, end_slice_date.toDateTime)
      (d.getMillis() / ObservationDStream.sliceTime.milliseconds).toInt
    }
    val all_current_indexes = {
      val current_index = start_index.copy(date = end_slice_date.toLocalDate(), slice = current_slice)
      // Find all the indexes that are spanned by the sequence of dates:
      // Find the slice corresponding to he start slice date:
      var previous_index = current_index.previousSlice
      while (previous_index.startTime.toDateTime() > begin_slice_date) {
        previous_index = previous_index.previousSlice
      }
      val all_indexes = {
        val x = DataSliceIndex.sliceRange(previous_index, current_index)
        if (x.last < current_index)
          x ++ Seq(current_index)
        else
          x
      }
      all_indexes
    }
    logInfo("Current indexes to process: " + all_current_indexes.mkString(", "))
    // Data can extend a bit over to the previous slice:
    val x = all_current_indexes.flatMap(index => {
      preloadedData(index) match {
        case Some(records_rdd) => {
          logInfo("Looking at index " + index)
          val rdd_data = records_rdd.filter(rec => rec.time >= begin_slice_date && rec.time <= end_slice_date)
          //          logInfo("Index %s has %d elements, will keep %d ".format(index.toString, records_rdd.count(), rdd_data.count()))
          Some(rdd_data)
        }
        case None => {
          logInfo("Trying to get index that does not exist: " + index)
          None
        }
      }
    })
    logInfo("Will filter %d RDD(s) for this day" format x.size)
    if (x.isEmpty) {
      logWarning("Could not find any data for this range of dates")
      Some(emptyRDD)
    } else {
      // Do not create a union RDD if not needed. This should be the most common case.
      val y = x.reduce(_ ++ _)
      // Apply the exponential weighting now:
      // TODO(tjh) will it prevent the object from being serialized as well?
      val half_time = decay_half_time
      val weighted = y.map(rec => {
        val d = new Duration(rec.time.toDateTime, end_slice_date.toDateTime)
        val h = d.getMillis().toDouble / half_time.milliseconds
        val w = math.exp(-h)
        (rec.obs, w)
      })
      Some(weighted)
    }
  }
}

class BatchStream(
  @transient val start_index: DataSliceIndex,
  @transient val context: SparkContext,
  @transient val window_length: Time,
  @transient val decay_half_time: Time,
  @transient val preloadedData: DataSliceIndex => Option[RDD[Record]] = (z: DataSliceIndex) => None) extends Stream with Serializable {
}

/**
 * Creates a DStream of observations.
 *
 * Starts from a given index, and expects the
 *
 * TODO(tjh) there is a lot of time manipulation in this class, move it to the object.
 *
 */
class ObservationDStream(
  @transient val start_index: DataSliceIndex,
  ssc: StreamingContext,
  @transient val slide_time: Time,
  @transient val window_length: Time,
  @transient val decay_half_time: Time,
  @transient preloaded_data: Map[DataSliceIndex, RDD[Record]] = Map.empty)
  extends InputDStream[(PartialObservation, Double)](ssc) with Stream {

  def preloadedData = preloaded_data.get _

//  override def context = ssc.context

  override val slideTime = slide_time

  def start = {}

  def stop = {}
}

object ObservationDStream extends MMLogging {

  //  val timePerStreamBatch = Seconds(2)

  // Time length of each slice of data stored on disk.
  // This is an intrinsic characteristic of the data
  // You should not have to change it unless you are Tim.
  // Each slice is 10 minutes
  val sliceTime = Seconds((24 * 3600. / DataSliceIndex.numSlicesPerDay).toInt)

  /**
   * The relative time compared to the start of a slice index.
   */
  def index(t: Time, reference_index: DataSliceIndex): DataSliceIndex = {
    require(t >= Time(0))
    if (t.milliseconds < sliceTime.milliseconds) {
      reference_index
    } else {
      index(t - sliceTime, reference_index.nextSlice)
    }
  }

  def indexStartTime(index: DataSliceIndex, reference_index: DataSliceIndex): Time = {
    require(index >= reference_index, (index, reference_index))
    var t = Time(0)
    var idx = reference_index
    while (idx <= index) {
      t = t + sliceTime
      idx = idx.nextSlice
    }
    t
  }

  /**
   * day_weights and slice_weights in decreasing order (oldest is last)
   */
  def assembleStream(
    day_weights: Array[Double],
    dstream: InputDStream[(PartialObservation, Double)]): DStream[(PartialObservation, Double)] = {

    // This is compact code, based on DataFlow.createDataFlowFromConfig
    val windowDelta = dstream.slideDuration * day_weights.size
    // Based on the slices, we assemble the complete sequence
    // Create auxiliary sequences
    // TODO(tjh): if we multiply by 7, we get the day of the weeks
    // Minus because we use days before
    val all_sequences = (0 until day_weights.size).map(i => {
      // This is insanely compact, should be expanded a bit...
      val milli_shift = 24 * 3600 * 1000 * i
      // Negative sign since we look in the past
      val shifted = new ShiftedInputDStream(dstream, Milliseconds(-milli_shift))
      // Add the day weights
      val d_w = day_weights(i)
      shifted.map({ case (pobs, w) => (pobs, w * d_w) })
    })
    all_sequences.fold(z => z._1.union(z._2))
//    new UnionDStream(all_sequences.toArray)
  }

  /**
   * Loads all the RDDs and returns a mapping to their sizes and indexed elements.
   */
  def preloadRDDs(
    sc: SparkContext,
    indexes: Seq[DataSliceIndex],
    num_splits: Int): DataSliceIndex => Option[RDD[Record]] = {
    val max_length = 6
    logInfo("Preloading data for %d indexes..." format indexes.size)
    val cache = collection.mutable.Map.empty[DataSliceIndex, RDD[Record]]
    def fun(index: DataSliceIndex) = {
      val fname = DataSliceIndex.fileName(index)
      if ((new File(fname).exists)) {
        val this_rdd = cache.getOrElseUpdate(index, {
          logInfo("Loading file %s ..." format fname)
          val data = Source.fromFile(fname)
            .getLines()
            .map(InputOutput.parseRecord _)
            .filter(_.obs.mask.nodes.size <= 6)
            .filter(_.obs.total <= 10 * 60) // Some crazy values
            .toArray
            .toIndexedSeq
          val indexed_rdd = sc.makeRDD(data, num_splits).cache
          indexed_rdd
        })
        // This will force the creation and the caching of the RDD, even if we could get the size from the data.
        // All these datasets are small
        Some(this_rdd)
      } else {
        None
      }
    }
    logInfo("Preloading data finished")
    fun
  }

  /**
   * Loads all the RDDs from HDFS and returns a mapping to their sizes and indexed elements.
   */
  def preloadDistributedRDDs = preloadDistributedRDDs1 _

  /**
   * Loads all the RDDs from HDFS and returns a mapping to their sizes and indexed elements.
   *
   * TODO(?) complicated code makes no performance difference........???????
   */
  def preloadDistributedRDDs0(
    sc: SparkContext,
    indexes: Seq[DataSliceIndex],
    num_splits: Int,
    num_parallel_batches: Int = 1): Map[DataSliceIndex, RDD[Record]] = {
    require(num_parallel_batches >= 1)
    require(sc != null)

    // Somehow sparkenv is not set
//    {
//      SparkEnv.set(sc.env)
//      logInfo("BC manager " + SparkEnv.get.broadcastManager)
//    }

    logInfo("Preloading HDFS data for %d indexes..." format indexes.size)

    val partitioner = new HashPartitioner(num_splits)

    val grouped_indexes = indexes
      .toSeq
      .grouped(indexes.size / num_parallel_batches)
      .toSeq
    logInfo("%d group indexes to process." format grouped_indexes.size)
    val futures = grouped_indexes.map(group_index => {
      Futures.future({
        logInfo("Starting processing group_index...")
//        if (SparkEnv.get == null) {
//          SparkEnv.set(sc.env) // FIXME!!!! it is a thread local value...  
//        }
        //        logInfo("SE "+SparkEnv.get)
        assert(SparkEnv.get != null)
        //        logInfo("BC manager "+SparkEnv.get.broadcastManager)
        val r = group_index.flatMap(index => {
          //          val to = 0 + (math.abs(index.hashCode()) % 5)
          //          logInfo("Timeout: "+to)
          //          Thread.sleep(to)
          val fname = DataSliceIndex.fileName(index)
          if ((new File(fname).exists)) {
            val hdfs_fname = DataSliceIndex.fileNameHDFS(index)

            val indexed_rdd = sc.textFile(hdfs_fname)
              .map(InputOutput.parseRecord _)
              .filter(_.obs.mask.nodes.size <= 5)
            val hack_rdd = indexed_rdd.map(z => (z.hashCode, z)).partitionBy(partitioner).map(_._2)
            //              .persist(StorageLevel.MEMORY_ONLY_2)
            //            .cache
            //        logInfo("Slice %s contains %d elements." format (index.toString, data.size))
            // This will force the creation and the caching of the RDD, even if we could get the size from the data.
            // All these datasets are small
            // Not forcing the computations at start for now. Huge impact on initial loading performance.
            hack_rdd.count()
            Some((index, hack_rdd))
            //            None
          } else {
            None
          }
        })
        logInfo("Finished processing group_index...")
        r
      })
    })

    val rdds = futures.flatMap(_.apply()).toMap

    //      .par
    //      .map(_).reduce(_ ++ _).toMap
    // Crashes
    //    val rdds = indexes
    //      .toSeq
    //      .grouped(indexes.size / num_parallel_batches)
    //      .toSeq
    //      .par
    //      .map(_.flatMap(index => {
    //        this.wait(10 + (index.hashCode()%10))
    //        val fname = DataSliceIndex.fileName(index)
    //        if ((new File(fname).exists)) {
    //          val hdfs_fname = DataSliceIndex.fileNameHDFS(index)
    //
    //          val indexed_rdd = sc.textFile(hdfs_fname, num_splits)
    //            .map(InputOutput.parseRecord _)
    //            .persist(StorageLevel.MEMORY_ONLY_2)
    //          //            .cache
    //          //        logInfo("Slice %s contains %d elements." format (index.toString, data.size))
    //          // This will force the creation and the caching of the RDD, even if we could get the size from the data.
    //          // All these datasets are small
    //          indexed_rdd.count().toInt
    //          Some((index, indexed_rdd))
    //        } else {
    //          None
    //        }
    //      })).reduce(_ ++ _).toMap
    logInfo("Preloading HDFS data finished")
    rdds
  }

  def completeSliding[T](x: IndexedSeq[T], size: Int): Seq[Seq[T]] = {
    val n = x.size
    (0 until n).map(i => x.slice(i, math.min(n, i + size)))
  }

  /**
   * Loads all the RDDs from HDFS and returns a mapping to their sizes and indexed elements.
   *
   * Serial, but very hacked version.
   */
  def preloadDistributedRDDs1(
    sc: SparkContext,
    indexes: Seq[DataSliceIndex],
    num_splits: Int,
    stream_initial_fraction: Double = 1.0): Map[DataSliceIndex, RDD[Record]] = {
    require(stream_initial_fraction >= 0.0)
    require(stream_initial_fraction <= 1.0)
    require(sc != null)

    logInfo("Preloading HDFS data for %d indexes..." format indexes.size)

    // Once we start experiments, these parameters should not be touched again.
    val min_length = 2
    val max_length = 5
    val aggregation_length: Int = 20

    // Preload all the data indexes into RDDs
    val all_simple_rdds = indexes.flatMap(index => {
      val fname = DataSliceIndex.fileName(index)
      if ((new File(fname).exists)) {
        val hdfs_fname = DataSliceIndex.fileNameHDFS(index)
        logInfo("HDFS file: " + hdfs_fname)
        val indexed_rdd = sc.textFile(hdfs_fname)
          .map(InputOutput.parseRecord _)
          .filter(rec => rec.obs.mask.nodes.size <= max_length &&
            rec.obs.mask.nodes.size >= min_length) // Remove elements that take too much or too little computations
          .filter(rec => {
            val h = rec.obs.hashCode
            val x = h.abs.toDouble / Int.MaxValue
            assert(x >= 0)
            assert(x <= 1.0)
            x <= stream_initial_fraction
          }) // Keep a random fraction of elements using a pseudo-random filter (hashcode)
          .cache() // Simple cache, we do not really care for these RDDs except that they will be accessed a number of times
        Some((index, indexed_rdd))
      } else {
        None
      }
    })

    logInfo("Starting aggregation step...")

    // Reassemble larger RDDs that contain all the k younger elements for the same slice.
    // This code is crashing, it should not!!!!
    val inflated_rdds = all_simple_rdds
      .groupBy(_._1.slice)
      .toSeq
      .sortBy(_._1) // Will be printed and processed in increasing order, easier to track
      .flatMap(z => {
        logInfo("Processing sequence for index %d" format z._1)
        // Put them in ordered sequences 
        val indexed_rdds = z._2.sortBy(_._1).toArray
        // Create a sliding window over these
        val aggregated_rdds = completeSliding(indexed_rdds, aggregation_length).map(z => {
          // For each window, aggregate everything into a single RDD by changing the date
          val master_index = z.head._1
          val master_date = master_index.date
          val rdds = z.map(_._2).toArray
          val repartitioned_rdd = Dummy.op(rdds, master_date, num_splits)
          (master_index, repartitioned_rdd)
        })
          .toSeq
        aggregated_rdds
      })
      .toMap

    // This code is crashing, it should not!!!!
    //    val partitioner = new HashPartitioner(num_splits)
    //    val inflated_rdds = all_simple_rdds.groupBy(_._1.slice).flatMap(z => {
    //      logInfo("Processing sequence for index %d" format z._1)
    //      // Put them in ordered sequences 
    //      val indexed_rdds = z._2.sortBy(_._1).toArray
    //      // Create a sliding window over these
    //      val aggregated_rdds = indexed_rdds.sliding(aggregation_length).map(z => {
    //        // For each window, aggregate everything into a single RDD by changing the date
    //        val master_index = z.head._1
    //        val master_date = master_index.date
    //        val combined_rdd = z.map(_._2).map(rdd => rdd.map(rec => {
    //          val new_date = rec.time
    //            .withDayOfMonth(master_date.getDayOfMonth())
    //            .withMonthOfYear(master_date.getMonthOfYear())
    //            .withYearOfEra(master_date.getYearOfEra())
    //          Record(new_date, rec.obs)
    //        }))
    //          .reduce(_ ++ _)
    //        
    //        // Repartition the RDD to make sure it does not have the many partitions from the combined RDDs.
    //        // Store them with replication
    //        val repartitioned_rdd = combined_rdd
    //          .map(z => (z.hashCode, z))
    //          .partitionBy(partitioner)
    //          .map(_._2)
    //          .persist(StorageLevel.MEMORY_ONLY_2)
    //        (master_index, repartitioned_rdd)
    //      })
    //        .toSeq
    //      aggregated_rdds
    //    })
    //      .toMap

    // These ones get shifter to be put in the same slice
    logInfo("Preloading HDFS data finished")
    //    all_simple_rdds.toMap
    inflated_rdds
  }

}

object Dummy {

  def shuffleRDD(rdd: RDD[Record], num_splits: Int): RDD[Record] = {
    val partitioner = new HashPartitioner(num_splits)
    rdd
      .map(rec => (rec.hashCode(), rec))
      .partitionBy(partitioner)
      .map(_._2)
      .persist(StorageLevel.MEMORY_ONLY_2)
  }

  def changeDate(rdd: RDD[Record], new_date: LocalDate): RDD[Record] = {
    val dom = new_date.getDayOfMonth()
    val moy = new_date.getMonthOfYear()
    val yoe = new_date.getYearOfEra()
    // TODO(tjh) WTF??!?!!?
    rdd.count // This line is necessary to prevent a crash in the closure serializer.
    rdd.map(rec => {
      val changed_date = rec.time
        .withDayOfMonth(dom)
        .withMonthOfYear(moy)
        .withYearOfEra(yoe)
      Record(changed_date, rec.obs)
    })
  }

  def op(rdds: Array[RDD[Record]], master_date: LocalDate, num_splits: Int): RDD[Record] = {
    val squeezed_rdds = rdds.map(changeDate(_, master_date))
    val combined_rdd = squeezed_rdds.reduce(_ ++ _)

    // Repartition the RDD to make sure it does not have the many partitions from the combined RDDs.
    // Store them with replication
    val repartitioned_rdd = shuffleRDD(combined_rdd, num_splits)
    repartitioned_rdd.count() // Force the creation of the RDD
    repartitioned_rdd

  }
}