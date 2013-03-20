package arterial_research.socc.io
import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import netconfig.io.files.Files
import java.io.File
import netconfig.io.Dates
import core_extensions.MMLogging
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import arterial_research.socc.Environment

/**
 * The index of a slice of data.
 * Each file holds a 10-minute slice of data.
 */
case class DataSliceIndex(
  val feed: String,
  val nid: Int,
  val date: LocalDate,
  val netType: String,
  val slice: Int) extends Ordered[DataSliceIndex] {

  override def toString = "%s-%d" format (date.toString, slice)

  def startTime:LocalDateTime = {
    date.toDateTimeAtMidnight.plusSeconds((slice * 24 * 3600) / DataSliceIndex.numSlicesPerDay).toLocalDateTime
  }
  
  /**
   * Returns a similar slice, shifted by a number of days.
   */
  def shiftByDays(num_days: Int): DataSliceIndex = {
    val new_date = date.plusDays(num_days)
    this.copy(date = new_date)
  }

  def nextSlice: DataSliceIndex = {
    val next_slice = (slice + 1) % DataSliceIndex.numSlicesPerDay
    if (next_slice == 0) {
      this.shiftByDays(1).copy(slice = 0)
    } else {
      this.copy(slice = next_slice)
    }
  }

  def previousSlice: DataSliceIndex = {
    val previous_slice = (slice - 1 + DataSliceIndex.numSlicesPerDay) % DataSliceIndex.numSlicesPerDay
    if (previous_slice == (DataSliceIndex.numSlicesPerDay - 1)) {
      this.shiftByDays(-1).copy(slice = DataSliceIndex.numSlicesPerDay - 1)
    } else {
      this.copy(slice = previous_slice)
    }
  }

  def compare(other: DataSliceIndex): Int = {
    require(feed == other.feed)
    require(nid == other.nid)
    require(netType == other.netType)
    val n = date.compareTo(other.date)
    if (n == 0) {
      slice.compareTo(other.slice)
    } else n
  }
}

object DataSliceIndex extends MMLogging {

  /**
   * The total number of slices per day.
   * 10 minutes per slice.
   */
  val numSlicesPerDay = 6 * 24

  def list(feed: String, nid: Int, dates: Seq[LocalDate] = Seq.empty[LocalDate], net_type: String, slices: Seq[Int] = Seq.empty): Seq[DataSliceIndex] = {
    val dic = dates.toSet
    val dir_name = "%s/%s/viterbi_slices_nid%d_%s/".format(Files.dataDir(), feed, nid, net_type)
    val dir = new File(dir_name)
    if (dir.exists()) {
      val regex = """(.*)/(\d\d\d\d-\d\d-\d\d)-(\d+).json""".r
      dir.listFiles().map(_.getAbsolutePath()).flatMap(p => p match {
        case regex(base, ymd, slicestr) => Dates.parseDate(ymd) match {
          case Some(date) => if (dic.isEmpty || dic.contains(date)) {
            Some(DataSliceIndex(feed, nid, date, net_type, slicestr.toInt))
          } else {
            None
          }
          case None => None
        }
        case _ => None
      }).toSeq.sortBy(_.date.toString)
    } else {
      logInfo("Directory: " + dir_name + " does not exist")
      Nil
    }
  }

  def fileName(feed: String, nid: Int, date: LocalDate, net_type: String, slice: Int) = {
    "%s/%s/viterbi_slices_nid%d_%s/%s-%d.json".format(Files.dataDir(), feed, nid, net_type, Dates.dateStr(date), slice)
  }

  def fileNameHDFS(feed: String, nid: Int, date: LocalDate, net_type: String, slice: Int) = {
    val hostname = Environment.hdfsRoot.getOrElse({logError("HDFS hostname not set") ; assert(false); ""})
    "%s/%s/viterbi_slices_nid%d_%s/%s-%d.json".format(hostname, feed, nid, net_type, Dates.dateStr(date), slice)
  }
  
  def fileName(index: DataSliceIndex): String = {
    import index._
    fileName(feed, nid, date, netType, slice)
  }

  def fileNameHDFS(index: DataSliceIndex): String = {
    import index._
    fileNameHDFS(feed, nid, date, netType, slice)
  }
  
  def sliceRange(from: DataSliceIndex, to: DataSliceIndex): Seq[DataSliceIndex] = {
    require(from <= to)
    val res = new ArrayBuffer[DataSliceIndex]()
    res += from
    while (res.last < to) {
      res += res.last.nextSlice
    }
    res.dropRight(1)
  }
}
