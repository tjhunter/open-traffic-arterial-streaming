package arterial_research.socc.io
import core_extensions.MMLogging
import netconfig.io.Dates
import org.joda.time.LocalDate
import scopt.OptionParser
import netconfig.io.files.RouteTTViterbi
import netconfig.io.files.SerializedNetwork
import netconfig.io.json.JSonSerializer
import netconfig.io.json.FileReading
import netconfig.storage.LinkIDRepr
import arterial_research.socc.NodeId
import arterial_research.socc.Record
import netconfig.storage.LinkIDReprOrdering
import com.codahale.jerkson.Json._
import netconfig.Datum.storage.PathInferenceRepr
import netconfig.io.StringDataSink
import netconfig.io.DataSink
import netconfig.io.DataSinks
import arterial_research.socc.PartialObservation
import arterial_research.socc.RecordRepr

object ConvertObservations extends MMLogging {
  
  def main(args:Array[String]) = {
    import Dates._
    var network_id:Int = -1
    var net_type:String = ""
    var date: LocalDate = null
    var range:Seq[LocalDate]=  Seq.empty
    var feed:String = ""
    var minute_per_slice:Int = 10
    var power_factor:Double = 2.0
    val parser = new OptionParser("test") {
      intOpt("nid", "the net id", network_id = _)
      opt("date", "the date", { s: String => { for (d <- parseDate(s)) { date = d } } })
      opt("range", "the date", (s: String) => for (r <- parseRange(s)) { range = r })
      opt("feed", "data feed", feed = _)
      opt("net-type", "The type of network", net_type = _)
      doubleOpt("skew-factor", ">1 puts less weight on the end of the link", power_factor = _)
      intOpt("minutes-per-slice", "The number of minutes per slice.", minute_per_slice = _)
    }
    parser.parse(args)

    assert(!net_type.isEmpty, "You must specify a network type")
    logInfo("Network type: " + net_type)

    val date_range: Seq[LocalDate] = {
      if (date != null) {
        Seq(date)
      } else {
        range
      }
    }
    assert(!date_range.isEmpty, "You must provide a date or a range of date")
    val indexes = RouteTTViterbi.list(feed=feed, nid=network_id, dates=date_range, net_type=net_type)
    logInfo("Number of indexes: " + indexes.size)
    
    // Loading the network topology
    val fname = SerializedNetwork.fileName(network_id, net_type)
    val glrs = JSonSerializer.getGenericLinks(fname)
    // Building the index of 
    val map:Map[LinkIDRepr, NodeId] = glrs.map(_.id).toSeq.sorted(LinkIDReprOrdering).zipWithIndex.toMap
    
    val lengths:Map[LinkIDRepr, Double] = glrs.map(glr => (glr.id, glr.length.get)).toSeq.toMap
    val slices_per_day = (60 * 24) / minute_per_slice
    
    def save(u:RecordRepr):String = generate(u)
    
    logInfo("About to start")
    for (index <- indexes) {
        logInfo("Started processing "+index)
        // Keep everything in memory to simplify
        val data_start = FileReading.readFlow(RouteTTViterbi.fileName(index))
            .map(s => parse[PathInferenceRepr](s))
        val data = data_start
            .flatMap(pi => ConvertTrajectories.convert(pi, lengths, map, slices_per_day))
        val data_grouped = data
            .groupBy(_._2)
        for ((slice, slice_data) <- data_grouped) {
          val fname = DataSliceIndex.fileName(index.feed, index.nid, index.date, index.netType, slice)
          val sink = DataSinks.map(StringDataSink.writeableFile(fname), save _)
          for ((obs, _) <- slice_data) {
            sink.put(obs)
          }
          sink.close()
        }
        logInfo("Finished processing "+ index + " : %d inputs, %d obs".format(data_start.size, data.size))
    }
  }
}