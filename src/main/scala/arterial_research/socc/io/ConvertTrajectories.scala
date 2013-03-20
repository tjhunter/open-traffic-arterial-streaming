package arterial_research.socc.io

import org.joda.time.LocalDate
import java.io.File
import netconfig.io.files.Files
import netconfig.io.Dates
import netconfig.Datum.RouteTT
import netconfig.Link
import netconfig.storage.RouteRepr
import netconfig.storage.LinkIDRepr
import arterial_research.socc.NodeId
import arterial_research.socc.Record
import arterial_research.socc.ObservationMask
import netconfig.Datum.storage.PathInferenceRepr
import netconfig.storage.LinkIDRepr
import arterial_research.socc.PartialObservation
import netconfig.storage.RouteRepr
import arterial_research.socc.ObservationMask
import org.joda.time.DateTime
import org.joda.time.LocalDateTime
import org.joda.time.Period
import core.storage.TimeRepr
import arterial_research.socc.PartialObservation
import netconfig.storage.SpotRepr
import netconfig.storage.SpotRepr
import core_extensions.MMLogging
import scopt.OptionParser
import netconfig.io.files.RouteTTViterbi
import netconfig.io.json.NetworkUtils
import netconfig.io.files.SerializedNetwork
import netconfig.io.json.JSonSerializer
import netconfig.storage.LinkIDReprOrdering
import collection.mutable.{ Map => MMap }
import arterial_research.socc.RecordRepr
import network.gen.GenericLinkRepresentation
import network.arterial1.ArterialLink

/**
 * The index class for a slice of data, ready in learning form.
 */
object ConvertTrajectories {

  def createIndexMap(glrs:Iterable[GenericLinkRepresentation]):Map[LinkIDRepr, NodeId] = {
    glrs.map(_.id).toSeq.sorted(LinkIDReprOrdering).zipWithIndex.toMap
  }
  
  def createArterialIndexMap(glrs:Iterable[ArterialLink]):Map[LinkIDRepr, NodeId] = {
    glrs.map(_.id).toSeq.sorted(LinkIDReprOrdering).zipWithIndex.toMap
  }
  
  val link_pre_cut_threshold = 20.0
  val link_post_cut_threshold = 20.0
  /**
   * Clip the start of a link.
   */
  def preClip(
    route: RouteRepr,
    lengths: Map[LinkIDRepr, Double],
    minimal_clip_fraction: Double = 0.3,
    minimal_extend_fraction: Double = 0.7): Option[RouteRepr] = {
    assert(route.spots.size == 2)
    assert(!route.links.isEmpty, route)
    val off = route.spots.head.offset
    val l = lengths(route.spots.head.linkId)
    // If the first offset is small, complete it
    if (off <= math.max(link_pre_cut_threshold, minimal_clip_fraction * l)) {
      val spots = Array(route.spots.head.copy(offset = off), route.spots.last)
      return Some(RouteRepr(route.links, spots))
    }
    // If it is high, remove it
    if (off >= math.min(minimal_extend_fraction * l, l - link_pre_cut_threshold)) {
      if (route.links.size == 1) {
        return None
      }
      val spots = Array(SpotRepr(route.links(1), 0.0), route.spots.last)
      return Some(RouteRepr(route.links.tail, spots))
    }
    None
  }
  // If it is close to intersection 

  /**
   * Clip the end of a link.
   */
  def postClip(
    route: RouteRepr,
    lengths: Map[LinkIDRepr, Double],
    minimal_clip_fraction: Double = 0.3,
    minimal_extend_fraction: Double = 0.7): Option[RouteRepr] = {
    assert(route.spots.size == 2)
    assert(!route.links.isEmpty, route)
    val off = route.spots.last.offset
    val l = lengths(route.links.last)
    val n = route.links.size
    // If the offset is too small, clip it.
    if (off <= math.max(link_pre_cut_threshold, minimal_clip_fraction * l)) {
      if (route.links.size == 1) {
        return None
      }
      val ante = route.links(n - 2)
      val spots = Array(route.spots.head, SpotRepr(ante, lengths(ante)))
      return Some(RouteRepr(route.links.slice(0, n - 1), spots))
    }
    // If it is high enough, keep the full link
    if (off >= math.min(minimal_extend_fraction * l, l - link_pre_cut_threshold)) {
      val spots = Array(route.spots.head, route.spots.last.copy(offset = l))
      return Some(RouteRepr(route.links, spots))
    }
    None
  }

  /**
   * Take a route (in representation form) and attemps to clip it or extend it to make it span
   * complete links.
   */
  def convertRoute(route: RouteRepr, lengths: Map[LinkIDRepr, Double], map: Map[LinkIDRepr, NodeId]): Option[ObservationMask] = {
    // We change the route representation to only keep the first and the last spot.
    // The inner spots are not necessary at this point
    val r = RouteRepr(route.links, Array(route.spots.head, route.spots.last))
    for (
      r1 <- preClip(r, lengths);
      r2 <- postClip(r1, lengths)
    ) {
      // We should still have 2 spots only
      assert(r2.spots.size == 2, (route, r2))
      assert(!r2.links.isEmpty, route)
      // Check that the output is still a route
      if (r2.links.toSet.size == 1) {
        // Make sure the offsets are in the right order
        if (r2.spots.head.offset >= r2.spots.last.offset) {
          return None
        }
      }
      val nodes = r2.links.map(map.apply _).toArray
      return Some(ObservationMask(nodes))
    }
    None
  }

  /**
   * Takes a route (in representation form) and attempts to build a mask:
   * - will clip or extend the route if some portions are too small
   * - uses the scaling factor for the elements in between
   * - removed the routes that are too small (less than fraction of the link and less than a certain length)
   */
  def convertRouteScaled(
    route: RouteRepr,
    lengths: Map[LinkIDRepr, Double],
    map: Map[LinkIDRepr, NodeId],
    minimal_single_link_fraction: Double = 0.60,
    minimal_clip_value: Double = 0.1,
    minimal_link_length: Double = 300,
    scaling_power: Double = 1.5): Option[ObservationMask] = {
    // We change the route representation to only keep the first and the last spot.
    // The inner spots are not necessary at this point
    val r = RouteRepr(route.links, Array(route.spots.head, route.spots.last))
    val parts = if (r.links.length == 1) {
      val x = route.spots.last.offset - route.spots.head.offset
      val l = r.links.head
      val y = x / lengths(l)
      assert(x >= 0)
      if (y <= minimal_single_link_fraction) {
        Seq.empty
      } else {
        Seq((l, y))
      }

    } else {
      val lid0 = route.links.head
      val l0 = lengths(lid0)
      val x0 = l0 - route.spots.head.offset
      val y0 = x0 / l0

      val lidn = route.links.last
      val ln = lengths(lidn)
      val xn = route.spots.last.offset
      val yn = xn / ln
      Seq((lid0, y0)) ++ route.links.drop(1).dropRight(1).map(lid => (lid, 1.0)) ++ Seq((lidn, yn))
    }
    val weighted_links = parts.flatMap({
      case (lid, y) =>
        val z = math.pow(y, scaling_power)
        if (z < minimal_clip_value) {
          None
        } else {
          Some((lid, z))
        }
    })

    if (weighted_links.isEmpty) {
      None
    } else {
      val nodes = weighted_links.map(z => map.apply(z._1)).toArray
      val weights = weighted_links.map(_._2).toArray
      Some(ObservationMask(nodes, weights))
    }
  }

  def convert(pir: PathInferenceRepr, lengths: Map[LinkIDRepr, Double], map: Map[LinkIDRepr, NodeId], num_slices_per_day: Int): Option[(RecordRepr, Int)] = {
    // Get the route from the PI object
    assert(pir.routes.size == 1, pir)
    val num_seconds_per_slice = 24 * 3600 / num_slices_per_day.toDouble
    for (mask <- convertRouteScaled(pir.routes.head, lengths, map)) {
      // Get the slice index from the time of day
      val period = new Period(pir.startTime.hour, pir.startTime.minute, pir.startTime.second, pir.startTime.milli)

      val start_time = TimeRepr.fromRepr(pir.startTime)
      val end_time = TimeRepr.fromRepr(pir.endTime)
      val tt = end_time - start_time
      val idx = math.floor(period.toStandardSeconds().getSeconds() / num_seconds_per_slice).toInt
      val pobs = new PartialObservation(mask, tt)
      val jt = {
        import pir.startTime._
        new LocalDateTime(year, month, day, hour, minute, second, milli)
      }
      val r = RecordRepr(jt, pobs)
      return Some((r, idx))
    }
    None
  }
}

