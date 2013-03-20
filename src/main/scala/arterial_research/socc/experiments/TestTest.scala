package arterial_research.socc.experiments

import arterial_research.socc.TestConfig
import netconfig.Datum.TrackPiece
import netconfig.Datum.storage.TrackPieceRepr
import netconfig.Link
import arterial_research.socc.testing.ErrorStatistics
import core.Time
import core.TimeUtils
import org.joda.time.Duration
import scala.collection.JavaConversions._
import netconfig.Route
import org.scala_tools.time.Imports._
import netconfig.io.files.TrajectoryViterbi
import scala.collection.mutable.ArrayBuffer
import netconfig.io.json.FileReading
import com.codahale.jerkson.Json._
import netconfig.Datum.storage.TrackPieceRepr
import netconfig.io.StringDataSink
import arterial_research.socc.testing.GammaTTProvider
import arterial_research.socc.testing.ArterialGammaTTProvider
import arterial_research.socc.EMConfig
import netconfig.io.DataSinks
import netconfig.io.json.JSonSerializer
import core_extensions.MMLogging
import netconfig.Datum.RouteTT
import netconfig.Datum.storage.PathInferenceRepr
import travel_time._
import arterial_research.socc.gamma.GammaMixtureRepr
import arterial_research.socc.gamma.GammaMixture
import arterial_research.socc.testing.GammaTTDistribution

case class TestResult(val routeTT: PathInferenceRepr, val stat: ErrorStatistics, val dis: GammaMixtureRepr)

object TestTest extends MMLogging {

  def trackStats[L <: Link](
    track: Iterable[TrackPiece[L]],
    min_interval: Duration,
    max_interval: Duration,
    tt_provider: ScalaTTProvider[L]): Seq[(RouteTT[L], ScalaTravelTimeDistribution, ErrorStatistics)] = {

    val track_times: Seq[Time] = track.map(_.point.time()).toIndexedSeq
    val indexed_pieces = (track_times zip track).toMap
    val split_times = TimeUtils.splitByInterval(min_interval, max_interval, track_times)
    split_times.flatMap(split_seq_times => {
      val split_seq = split_seq_times.map(indexed_pieces.apply _).toIndexedSeq
      val routes = split_seq.flatMap(_.routes.headOption)
      def cat(r1: Route[L], r2: Route[L]): Route[L] = r1.concatenate(r2)
      val complete_route = routes.reduce(cat _)
      val end_dt = split_seq.last.point.time().toDateTime()
      val start_dt = split_seq.head.point.time().toDateTime()
      val complete_interval = start_dt to end_dt
      val ttd_either = tt_provider.travelTimeSafe(start_dt, complete_route)
      for (failure <- ttd_either.right) {
        logInfo("Failed to process track: " + failure.getMessage())
      }
      ttd_either.left.map(ttd => {
        val es = ErrorStatistics.getErrorStat(ttd, complete_interval.toDuration().seconds)
        val start_t = Time.from(start_dt)
        val end_t = Time.from(end_dt)
        val rtt = RouteTT.from(complete_route, start_t, end_t, track.head.point().id())
        (rtt, ttd, es)
      }).left.toOption
    })
  }
}

object TestStream extends App with MMLogging {
  val em_config: EMConfig = SlidingBig.em_config
  val config: TestConfig = new TestConfig {
    start_time = new DateTime(2010, 10, 12, 0, 0, 0)
    end_time = new DateTime(2010, 10, 12, 23, 0, 0)
    slice_window_duration = (SlidingBig.config.slice_window_duration.value.milliseconds / 1000).toInt.seconds.toDuration
    states_directory = "/windows/D/arterial_experiments/tase/states/SlidingBig/"
    test_output_directory = "/windows/D/arterial_experiments/tase/perf/SlidingBig/"
    network_id = SlidingBig.config.network_id
    network_type = SlidingBig.config.network_type
    source_name = SlidingBig.config.source_name

    split_intervals = Seq(
      (50 seconds, 70 seconds),
      (3.minutes + 50.seconds, 3.minutes + 70.seconds),
      (10.minutes + 50.seconds, 10.minutes + 70.seconds),
      (20.minutes + 50.seconds, 20.minutes + 70.seconds)).map(z => (z._1.toDuration, z._2.toDuration))
  }

  em_config.save_states_dir = Some(config.states_directory)
  em_config.save_full_states_dir = Some(config.states_directory)

  logInfo("saved states: " + em_config.save_states_dir.get)

  val anet = ArterialGammaTTProvider.loadArterialNetwork(config.network_id, config.network_type)
  val codec = JSonSerializer.from(anet)
  val tt_provider = ArterialGammaTTProvider.createFromConfig(em_config, 4, anet)

  val dates = {
    val start_date = config.start_time.toLocalDate()
    val end_date = config.end_time.toLocalDate()
    val buff = new ArrayBuffer[LocalDate]
    buff.append(start_date)
    while (buff.head < end_date) {
      buff.append(buff.head + 1.day)
    }
    buff.toSeq
  }
  logInfo("dates: " + dates.mkString(" "))

  val indexes = TrajectoryViterbi.list(config.source_name, config.network_id, config.network_type, dates).view
  logInfo("%d indexes " format indexes.size)

  val tracks = indexes.map(index => {
    val fname = TrajectoryViterbi.fileName(index)
    (index, codec.readTrack(fname))
  })

  for ((start_duration, end_duration) <- config.split_intervals) {
    val output_name = "%s/%s.txt" format (config.test_output_directory.value, start_duration.toString())
    val output = DataSinks.map(StringDataSink.writeableFile(output_name), (x: TestResult) => generate(x))
    for ((findex, track) <- tracks.take(20000)) { // DEBUG
      val stats = TestTest.trackStats(track, start_duration, end_duration, tt_provider)
      for ((rtt, ttd, stat) <- stats) {
        val rtt_r = codec.toRepr(rtt, true)
        val gm = ttd.asInstanceOf[GammaTTDistribution].mix.repr
        val test_res = new TestResult(rtt_r, stat, null)
        output.put(test_res)
      }
    }
    output.close()
  }
}

object Test extends App with MMLogging {

  def runTests(em_config: EMConfig, config: TestConfig): Unit = {
    em_config.save_states_dir = Some(config.states_directory)
    em_config.save_full_states_dir = Some(config.states_directory)

    logInfo("saved states: " + em_config.save_states_dir.get)

    val anet = ArterialGammaTTProvider.loadArterialNetwork(config.network_id, config.network_type)
    val codec = JSonSerializer.from(anet)
    val em_iter = config.em_iter.getOrElse(4)
    val tt_provider = ArterialGammaTTProvider.createFromConfig(em_config, em_iter, anet)

    val dates = {
      val start_date = config.start_time.toLocalDate()
      val end_date = config.end_time.toLocalDate()
      val buff = new ArrayBuffer[LocalDate]
      buff.append(start_date)
      while (buff.head < end_date) {
        buff.append(buff.head + 1.day)
      }
      buff.toSeq
    }
    logInfo("dates: " + dates.mkString(" "))

    val indexes = TrajectoryViterbi.list(config.source_name, config.network_id, config.network_type, dates).view
    logInfo("%d indexes " format indexes.size)

    val tracks = indexes.map(index => {
      val fname = TrajectoryViterbi.fileName(index)
      (index, codec.readTrack(fname))
    })

    for ((start_duration, end_duration) <- config.split_intervals) {
      val output_name = "%s/%s.txt" format (config.test_output_directory.value, start_duration.toString())
      val output = DataSinks.map(StringDataSink.writeableFile(output_name), (x: TestResult) => generate(x))
      for ((findex, track) <- tracks.take(20000)) { // DEBUG
        val stats = TestTest.trackStats(track, start_duration, end_duration, tt_provider)
        for ((rtt, ttd, stat) <- stats) {
          val rtt_r = codec.toRepr(rtt, true)
          val gm = ttd.asInstanceOf[GammaTTDistribution].mix.repr
          val test_res = new TestResult(rtt_r, stat, null)
          output.put(test_res)
        }
      }
      output.close()
    }

  }
}