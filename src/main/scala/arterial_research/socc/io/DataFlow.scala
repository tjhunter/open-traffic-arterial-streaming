package arterial_research.socc.io

import netconfig.io.Dates
import core_extensions.MMLogging
import scala.collection.mutable.ArrayBuffer
import arterial_research.socc.DataDescription

/**
 * Describes the flow of data slices used in experiments.
 */
trait DataFlow {
  /**
   * Each cell is a weighted combination of slices.
   */
  def flow: Seq[Seq[DataSliceIndex]]
}

object DataFlow extends MMLogging {

//  def previousIndex(index: DataSliceIndex, num_slices: Int): DataSliceIndex = {
//    if (index.slice > 0)
//      index.copy(slice = index.slice - 1)
//    else {
//      index.copy(date = index.date.minusDays(1), slice = num_slices - 1)
//    }
//  }
//
//  def createEnvelope(slice: DataSliceIndex, slices: Seq[DataSliceIndex], config: DataDescription): Seq[(Double, DataSliceIndex)] = {
//    val num_slices = 24 * 6 // 10-minute slices
//    // The master list for the same dame
//    val list = {
//      val x = new ArrayBuffer[DataSliceIndex]()
//      x += slice
//      for (i <- 1 until config.slice_window_size) {
//        x.append(previousIndex(x.last, num_slices))
//      }
//      x.toSeq.zipWithIndex
//    }
//
//    (0 until config.day_window_size).flatMap(day =>
//      for ((index, j) <- list) yield {
//        val shifted_index = index.copy(date = index.date.minusDays(day))
//        val w = math.pow(config.day_exponential_weighting, day) * math.pow(config.slice_exponential_weighting, j)
//        (w, shifted_index)
//      }).filter(z => slices.contains(z._2))
//  }
//
//  def createDataFlowFromConfig(config: DataDescription): DataFlow = {
//    // Get the dates
//    val range = Dates.parseRange(config.date_range) match {
//      case None => assert(false, config.date_range); null
//      case Some(x) => x
//    }
//    // Read all the slice indexes in disk and order them, and put them in a dictionary
//    val valid_slices = DataSliceIndex.list(config.source_name,
//      config.network_id, net_type = config.network_type)
//      .sorted
//      .toSet
//      
//    logInfo("Number of valid slices "+valid_slices.size)
//
//    val start_slice = DataSliceIndex(config.source_name, config.network_id, range.head, config.network_type, 0)
////     logInfo("start_slice "+start_slice)
//    val end_slice = start_slice.copy(date = range.last, slice = DataSliceIndex.numSlicesPerDay)
////     logInfo("end_slice "+end_slice)
//    val main_sequence = DataSliceIndex.sliceRange(start_slice, end_slice)
////     logInfo("Size of main sequence: "+main_sequence.size)
//    // Create the sequences of target indexes
//    val day_weights = (0 until config.day_window_size).map(day => math.pow(config.day_exponential_weighting, day)).toArray
//    val slice_weights = (0 until config.slice_window_size).map(slice => math.pow(config.slice_exponential_weighting, slice)).toArray
////     logInfo("day_weights size "+day_weights.size)
////     logInfo("slice_weights size "+slice_weights.size)
//    // Based on the slices, we assemble the complete sequence
//    // Create auxiliary sequences
//    // TODO(tjh): if we multiply by 7, we get the day of the weeks
//    // Minus because we use days before
//    val all_sequences = (0 until day_weights.size).map(i => {
//      // This is insanely compact, should be expanded a bit...
//      val s = main_sequence.map(_.shiftByDays(-i))
//        // For each sequence, create the windowed sequences    
//        .sliding(slice_weights.size).toSeq
//        // Add the daily slice weights 
//        // Make sure to reverse to get it in the good order (most recent first)
//        // Because day weights are in decreasing order
//        .map(seq => slice_weights.zip(seq.reverse))
//        // Filter to keep only the valid slices
//        .map(seq => seq.filter(z => valid_slices.contains(z._2)))
//      // Add the day weights
////       for(seq <- s) { logInfo("seq : "+seq.toList)}
//      (day_weights(i), s)
//    }).toArray
//    // We need to apply cross sum which is not possible directly??
//    val size_sequence = all_sequences.head._2.size
////     logInfo("size_sequence: "+size_sequence)
//    // This is terrible from a readability perspective
//    val reduced_sequence = (0 until size_sequence).map(idx => {
//      all_sequences.flatMap({
//        case (day_w, seq) =>
////           logInfo("seq size "+seq.size)
//          seq(idx).map({
//            case (slice_w, index) =>
//              (slice_w * day_w, index)
//          }).toIterable
//      }).toSeq
//    })
//    new DataFlow {
//      def flow = reduced_sequence
//    }
//  }
  def createDataFlowFromConfig(config: DataDescription): DataFlow = {
    assert(false)
    null
  }

}