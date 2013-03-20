package arterial_research.socc

import scalax.io._
import arterial_research.socc.gamma.GammaState
import arterial_research.socc.gamma.GammaSample

object TestOutput extends App {

  if (true) {
    val gs1 = new GammaState(20, 2)
    val gs2 = new GammaState(4, 20)
    val num_samples = 10000
    val state = Map(0->gs1, 1->gs2)
    val keys = Array(0, 1)
    val tts = Array(15.0, 0)
    val c_obs = new CompleteObservation(new ObservationMask(keys), tts)
    val p_obs = c_obs.toPartial
    val samples = GammaSample.getSamples(p_obs, state, num_samples)
    val output = Resource.fromFile("/tmp/1.txt")
    for (processor <- output.outputProcessor) {
      val out = processor.asOutput
      for (cobs <- samples) {
        out.writeStrings(cobs.partial.map(_.toString()), " ")(Codec.UTF8)
        out.write("\n")(Codec.UTF8)
      }
    }
  }
//  val (alpha, beta, gamma) = (20,2,-4)
//  val num_samples = 1000
//  val output = Resource.fromFile("/tmp/1.txt")
//  output.openOutput { out =>
//    for (i <- 0 until num_samples) {
//      out.write("%f\n" format GammaDis.sample_auxi(alpha, beta, gamma))(Codec.UTF8)
//    }
//  }

}