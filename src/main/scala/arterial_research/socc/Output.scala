package arterial_research.socc
import scalax.io._
import com.codahale.jerkson.Json._
import netconfig.storage._
import netconfig.Datum.storage.PathInferenceRepr
import core.storage.TimeRepr
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import network.gen.GenericLinkRepr
import network.gen.GenericLinkRepresentation
import network.gen.GenericLinkRepresentation
import network.gen.GenericLinkRepresentation
import core_extensions.MMLogging
import arterial_research.socc.gamma.GammaState
import org.joda.time.chrono.ISOChronology
import org.joda.time.LocalDateTime
import org.joda.time.DateTime
import io.DataSliceIndex
import processing.Processor
import java.io.BufferedReader
import java.io.FileReader
import java.io.FileWriter
import java.io.BufferedWriter

/**
 * Some I/O functions to manipulate disk objects.
 */
object InputOutput extends MMLogging {
  
  def readLine(s:String): (NodeId, GammaState) = {
    val s2 = s.replaceAll("_1","nodeId")
    .replaceAll("_2","state")
    parse[StateRecord](s2).pair
  }
  
  def readStates(fname:String): Map[NodeId, GammaState] = {
    import scalax.io.Codec.UTF8
    val out = Resource.fromReader(new BufferedReader(new FileReader(fname)))
    val config = out.lines()
    .map(readLine _)
    Map.empty ++ config
  }
  
  def writeState(fname:String, params:Map[NodeId, GammaState]): Unit = {
    val out = new BufferedWriter(new FileWriter(fname))
      for (x <- params.toSeq.sortBy(_._1)) {
        val sr = StateRecord(x._1, x._2)
        out.write(generate(sr))
        out.write("\n")
      }
    out.close()
//    import scalax.io.Codec.UTF8
//    val output:Output = Resource.fromFile(fname)
//    for (processor <- output.outputProcessor) {
//      val out = processor.asOutput
//      for (x <- params.toSeq.sortBy(_._1)) {
//        val sr = StateRecord(x._1, x._2)
//        out.write(generate(sr))
//        out.write("\n")
//      }
//    }
  }
  
  case class ObsCountsRecord(val nodeId:Int, val count:Double)
  
  def writeObsCounts(fname:String, params:Map[NodeId, Double]): Unit = {
    import scalax.io.Codec.UTF8
    val output = Resource.fromFile(fname)
    for (processor <- output.outputProcessor) {
      val out = processor.asOutput
      for (x <- params.toSeq.sortBy(_._1)) {
        val sr = ObsCountsRecord(x._1, x._2)
        out.write(generate(sr))
        out.write("\n")
      }
    }
  }
  
  def parseObservation(ids:Map[LinkIDRepr, NodeId], s_obs:String):PartialObservation = {
    val obs = parse[PathInferenceRepr](s_obs)
    val tt = TimeRepr.fromRepr(obs.endTime) - TimeRepr.fromRepr(obs.startTime)
    val route = obs.routes.head.links.map(ids.apply _)
    new PartialObservation(ObservationMask(route.toArray), tt)
  }
  
  def parseObservation(s_obs:String):PartialObservation = {
    parse[PartialObservation](s_obs)
  }
  
  
  def parseRecord(s_obs:String):Record = {
    val rec = parse[RecordRepr](s_obs)
    rec.toRecord
  }
  
  def loadNetwork(fname:String):Map[LinkIDRepr, GenericLinkRepresentation] = {
    import scalax.io.Codec.UTF8
    val in = Resource.fromInputStream(new GZIPInputStream(new FileInputStream(fname)))
    val links = in.lines().map(s => parse[GenericLinkRepresentation](s))
    Map.empty ++ links.map(l => (l.id, l))
  }
  
  /**
   * Reads a network description in standard form, and creates a map from link ids to node ids.
   */
  def loadNodeIds(fname:String):Map[LinkIDRepr, NodeId] = {
    Map.empty ++ loadNetwork(fname).keys.zipWithIndex
  }
  
  def partialStateFilename(config:EMConfig, time:DateTime, slice_index:DataSliceIndex, step:Int):String = {
     val tstring = if(time == null) {slice_index.toString()} else {time.toString()}
     config.save_states_dir.map(save_dir => {
       "%s/%s-%d.txt" format (save_dir, tstring, step)
     }).getOrElse("")
  }
  
   def saveStats(config:EMConfig, stats: StepStatistics, time:DateTime): Unit = {
     logInfo("Logging step...")
     implicit val codec = Codec.UTF8
     val tstring = if(time == null) {stats.slice_index.toString()} else {time.toString()}
     for (save_dir <- config.save_stats_dir) {
       val fname = "%s/perf%s.txt" format (save_dir, stats.slice_index.toString)
      logInfo("Logging info in "+fname)
       val file = Resource.fromFile(fname)
       file.append("%s " format tstring)
       file.append("%s " format stats.slice_index.toString())
       file.append("%d " format stats.step)
       file.append("%f " format stats.total_time)
       file.append("%f " format stats.marginal_ll)
       file.append("%f " format stats.ecll)
       file.append("%f " format stats.entropy)
       file.append("%d " format stats.obs_count)
       file.append("\n")
     }

     for (save_dir <- config.save_states_dir) {
       val fname = "%s/%s-%d.txt" format (save_dir, tstring, stats.step)
       InputOutput.writeState(fname, stats.state)
     }
     for (save_dir <- config.save_states_dir) {
       val fname = "%s/counts-%s-%d.txt" format (save_dir, tstring, stats.step)
       InputOutput.writeObsCounts(fname, stats.observed_weights)
     }
     for (save_dir <- config.save_full_states_dir) {
       val fname = "%s/full-%s-%d.txt" format (save_dir, tstring, stats.step)
       InputOutput.writeState(fname, stats.full_state)
     }
     logInfo("Logging step done.")
   }
  
}
