package arterial_research.socc_test.gamma

import arterial_research.socc.gamma._
import arterial_research.socc._
import org.junit._
import org.junit.Assert._
import math._
import java.io.File
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import breeze.linalg.DenseVector._
import breeze.linalg.DenseVector
import core_extensions.MMLogging

//@RunWith(classOf[JUnitRunner])
class GammaSampleTest extends FunSuite with MMLogging with TestingMath {

  // Not too aggressive, otherwise tests will fail
  val epsi = 1e-2

  test("Reference test #1") { ref1 }

//  @Test 
  def ref1: Unit = {
    val alpha = 2.0
    val beta = 3.0
    val g = 0.5
    val N = 10000
    val xs = DenseVector((0 until N).map(i => GammaSample.sample_auxi(alpha, beta, g)).toArray)
    val moments = (1 to 4).map(i => (xs :^ i.toDouble).sum / N)
    println("moments: "+moments)
    val true_moments = Array(0.42522777291420127, 0.22178628886295831, 0.13079886413204239, 0.083814502150802106)
    for ((a, b) <- moments zip true_moments) {
      assert(abs(a - b) < epsi)
    }
  }

  test("Reference test #2 (gamma_gen_test_5)") { ref2 }

  @Test def ref2: Unit = {
    val alpha = 1.509619
    val beta = 1.393222
    val g = 0.33160171594
    val N = 1
    val xs = DenseVector((0 until N).map(i => GammaSample.sampleAuxi2(alpha, beta, g)).toArray)
  }
  
  test("Pair sampling procedure (gamma_gen_test_4)") { pairSampler1 }
  
  @Test
  def pairSampler1: Unit = { 
    val link_state_1 = GammaState(k = 1.509619, theta = 15.603218)
    val link_state_2 = GammaState(k = 1.393222, theta = 12.402064)
    val total_t = 20.04556705118357
    val true_moments1 = DenseVector(Array(10.923962915289735, 144.71781723186243, 2114.2315334054329, 32790.958723479584))
    val true_moments2 = DenseVector(Array(9.1216041358936835, 108.58851346918755, 1474.8694527288658, 21675.85656283006))
    val num_samples = 10000
    
    val samples = (1 to num_samples).map(i => GammaSample.sampler_pair(link_state_1, link_state_2, total_t))
    
    val x1s = DenseVector(samples.map(_._1).toArray)
    val moments1 = DenseVector((1 to 4).map(i => (x1s :^ i.toDouble).sum / samples.length).toArray)
    val diff1 = (moments1 - true_moments1)
    logInfo("Diff1 = " + diff1)
    val rdiff1 = (moments1 - true_moments1) :/ true_moments1
    assert(rdiff1.data.forall(math.abs(_) < 1e-1))
    
    val x2s = DenseVector(samples.map(_._2).toArray)    
    val moments2 = DenseVector((1 to 4).map(i => (x2s :^ i.toDouble).sum / samples.length).toArray)
    val diff2 = (moments2 - true_moments2)
    logInfo("Diff2 = " + diff2)
    val rdiff2 = diff2 :/ true_moments2
    assert(rdiff2.data.forall(math.abs(_) < 1e-1))
  }
  

  test("Full sampling procedure (gamma_gen_test_3)") { fullSampler1 }
  
  @Test
  def fullSampler1: Unit = {
    val link_state_1 = GammaState(k = 1.509619, theta = 15.603218)
    val link_state_2 = GammaState(k = 1.393222, theta = 12.402064)
    val total_t = 20.04556705118357
    val proj_idxs = Array(0, 1)
    val state_map = Map(0 -> link_state_1, 1 -> link_state_2)
    val num_samples = 10000
    val pobs = PartialObservation(ObservationMask(proj_idxs), total_t)
    val samples = GammaSample.getSamples(pobs, state_map, num_samples)

    val true_moments1 = DenseVector(Array(10.925764876067632, 144.83414909634413, 2117.1495155714219, 32848.235360825282))
    val true_moments2 = DenseVector(Array(9.1198021751160496, 108.63260268248247, 1476.7750677693668, 21721.575139846482))
    
    val x1s = DenseVector(samples.map(_.partial(0).toDouble).toArray)
    val moments1 = DenseVector((1 to 4).map(i => (x1s :^ i.toDouble).sum / samples.length).toArray)
    val diff1 = (moments1 - true_moments1)
    logInfo("Diff1 = " + diff1)
    val rdiff1 = (moments1 - true_moments1) :/ true_moments1
    assert(rdiff1.data.forall(math.abs(_) < 1e-1))
    
    val x2s = DenseVector(samples.map(_.partial(1).toDouble).toArray)    
    val moments2 = DenseVector((1 to 4).map(i => (x2s :^ i.toDouble).sum / samples.length).toArray)
    val diff2 = (moments2 - true_moments2)
    logInfo("Diff2 = " + diff2)
    val rdiff2 = diff2 :/ true_moments2
    assert(rdiff2.data.forall(math.abs(_) < 1e-1))
  }
  
  @Test
  def testBeta1: Unit = {
    val a = 0.0014364182264741652
    val b = 0.0024709345620239687
    val n = 100000
    val samples = (0 until n).map(i=>GammaSample.sampleBeta(a,b)).toArray
    val mean = samples.sum / n
    val true_mean = a / (a+b)
    assert(math.abs(mean - true_mean) < 1e-2, (mean, true_mean))
  }

  @Test
  def testBeta2: Unit = {
    val a = 3.0
    val b = 4.0
    val n = 100000
    val samples = (0 until n).map(i=>GammaSample.sampleBeta(a,b)).toArray
    val mean = samples.sum / n
    val true_mean = a / (a+b)
    assert(math.abs(mean - true_mean) < 1e-2, (mean, true_mean))
  }
  
  @Test
  def testBeta3: Unit = {
    val a = 7.672385302336129E-4
    val b = 0.4028709732819038
    val n = 100000
    val samples = (0 until n).map(i=>GammaSample.sampleBeta(a,b)).toArray
    val mean = samples.sum / n
    val true_mean = a / (a+b)
    assert(math.abs(mean - true_mean) < 1e-2, (mean, true_mean))
  }
  
  @Test
  def failure1:Unit = {
    val gs =  GammaState(k=0.657165, theta=0.138882)
    val n = 500000
    val samples = (0 until n).map(i => GammaSample.sampleState(gs))
    val emp_mean = samples.sum / n
    assertClose(emp_mean, gs.mean, 0.1)
  }
  
}