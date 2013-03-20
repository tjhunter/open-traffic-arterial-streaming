package arterial_research.socc_test.gamma

import arterial_research.socc.gamma._
import org.junit._
import org.junit.Assert._
import math._
import java.io.File
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import arterial_research.socc._
import breeze.linalg.DenseVector._
import breeze.linalg.DenseVector
import core_extensions.MMLogging

class GammaLearnTest extends FunSuite with MMLogging with TestingMath {
  test("Very long test for EM stability (MCMC)") { chain3 }

  @Test
  def chain3: Unit = {
    val gs1 = new GammaState(2, 5)
    val gs2 = new GammaState(4, 5)
    val gs3 = new GammaState(6, 5)
    val true_state = Map(0 -> gs1, 1 -> gs2, 2 -> gs3)
    println("Target state: " + true_state)
    val nodes = (0 to 2).toArray

    val num_samples = 10
    val num_obs = 100
    val num_em_steps = 10
    val obs = Sampler.generateSyntheticSamples(num_obs, 2, true_state)
    val p_obs = obs.map(_.toPartial).map((1.0, _))
    var state: Map[NodeId, GammaState] = true_state //new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    //    var state:Map[NodeId,GammaState] = true_state // new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    for (em_step <- 1 to num_em_steps) {
      println("EM STEP %d" format em_step)
      println(state.toSeq.sortBy(_._1))
      state = LearnSpark.learnWeightedStep(p_obs, state, num_samples, 0.0, state)
      assertEquals(state.size, true_state.size)
    }
    val epsi = 1.0
    for (nid <- nodes) {
      assertTrue(true_state(nid).isApprox(state(nid))(epsi))
    }
  }

  @Test
  def chain2: Unit = {
    val gs1 = new GammaState(2, 5)
    val gs2 = new GammaState(4, 5)
    val gs3 = new GammaState(6, 5)
    val gs_start = new GammaState(1, 10)
    val true_state = Map(0 -> gs1, 1 -> gs2, 2 -> gs3)
    println("Target state: " + true_state)
    val nodes = (0 to 2).toArray

    val num_samples = 10
    val num_obs = 100
    val num_em_steps = 100
    val obs = Sampler.generateSyntheticSamples(num_obs, 2, true_state)
    val p_obs = obs.map(_.toPartial).map((1.0, _))
    var state: Map[NodeId, GammaState] = Map(0 -> gs_start, 1 -> gs_start, 2 -> gs_start)
    //    var state:Map[NodeId,GammaState] = true_state // new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    for (em_step <- 1 to num_em_steps) {
      println("EM STEP %d" format em_step)
      println(state.toSeq.sortBy(_._1))
      state = LearnSpark.learnWeightedStep(p_obs, state, num_samples, 0.0, state)
      assertEquals(state.size, true_state.size)
    }
    val epsi = 1.0
    for (nid <- nodes) {
      assertTrue(true_state(nid).isApprox(state(nid))(epsi))
    }
  }

  @Test
  def chain1: Unit = {
    val gs1 = new GammaState(2, 5)
    val true_state = Map(0 -> gs1)
    println("Target state: " + true_state)
    val nodes = (0 until true_state.size).toArray

    val num_samples = 2
    val num_obs = 1000
    val num_em_steps = 0
    val obs = Sampler.generateSyntheticSamples(num_obs, 1, true_state)
    val p_obs = obs.map(_.toPartial).map((1.0, _))
    var state: Map[NodeId, GammaState] = true_state //new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    //    var state:Map[NodeId,GammaState] = true_state // new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    for (em_step <- 1 to num_em_steps) {
      logInfo("EM STEP %d" format em_step)
      logInfo(state.toSeq.sortBy(_._1).mkString(" "))
      state = LearnSpark.learnWeightedStep(p_obs, state, num_samples, 0.0, state)
      assertEquals(state.size, true_state.size)
    }
    val epsi = 0.1
    logInfo("Learned state: " + state.toSeq.sortBy(_._1).mkString(" "))
    logInfo("True state: " + true_state.toSeq.sortBy(_._1).mkString(" "))
    for (nid <- nodes) {
      assertTrue(true_state(nid).isApprox(state(nid))(epsi))
    }
  }

  @Test
  def scaledChain1: Unit = {
    val gs1 = new GammaState(2, 5)
    val true_state = Map(0 -> gs1)
    println("Target state: " + true_state)
    val nodes = (0 until true_state.size).toArray

    val num_samples = 100
    val num_obs = 1000
    val num_em_steps = 1
    val obs = Sampler.generateScaledSyntheticSamples(num_obs, 1, true_state, .2,.4)
    val p_obs = obs.map(_.toPartial).map((1.0, _))
    var state: Map[NodeId, GammaState] = true_state //new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    //    var state:Map[NodeId,GammaState] = true_state // new LinearState(Array(new GammaState(1, 10), new GammaState(1, 10), new GammaState(1, 10)))
    for (em_step <- 1 to num_em_steps) {
      logInfo("EM STEP %d" format em_step)
      logInfo(state.toSeq.sortBy(_._1).mkString(" "))
      state = LearnSpark.learnWeightedStep(p_obs, state, num_samples, 0.0, state)
      assertEquals(state.size, true_state.size)
    }
    val epsi = 0.1
    logInfo("Learned state: " + state.toSeq.sortBy(_._1).mkString(" "))
    logInfo("True state: " + true_state.toSeq.sortBy(_._1).mkString(" "))
    for (nid <- nodes) {
      assertTrue(true_state(nid).isApprox(state(nid))(epsi))
    }
  }
  
  @Test
  def reg1: Unit = {
    val gs1 = new GammaState(2, 5)
    val true_state = Map(0 -> gs1)
    println("Target state: " + true_state)
    val nodes = (0 to true_state.size).toArray

    val num_obs = 100000
    val obs = Sampler.generateSyntheticSamples(num_obs, 1, true_state)

    val stats1 = gs1.sufficientStatistics

    val ts = obs.map(_.toPartial.total).toArray
    val ws = Array.fill(ts.size)(1.0)

    val stats2 = GammaLearn.sufficientStats(ts, ws)
    logInfo("stats1: " + stats1)
    logInfo("stats2: " + stats2)
    assertClose(stats1.mean, stats2.mean, 1e-1)
    assertClose(stats1.meanOfLogs, stats2.meanOfLogs, 1e-1)
  }

  def checkLearn(gs: GammaState, precision: Double = 5e-2, num_samples: Int = 100000): Unit = {
    val stats1 = gs.sufficientStatistics

    val ts = (0 until num_samples).map(i => GammaSample.sampleState(gs)).toArray
    val mean = ts.sum / num_samples
    logInfo("expected mean: " + gs.mean)
    logInfo("empirical mean: " + mean)
    assertClose(gs.mean, mean, precision)

    val ws = Array.fill(ts.size)(1.0)
    val stats2 = GammaLearn.sufficientStats(ts, ws)
    logInfo("stats1: " + stats1)
    logInfo("stats2: " + stats2)
    assertClose(stats1.mean, stats2.mean, precision)
    assertClose(stats1.meanOfLogs, stats2.meanOfLogs, precision)
    val gs2 = GammaLearn.mle(stats2)
    logInfo("gs2: " + gs2)
    assertClose(gs.k, gs2.k, precision)
    assertClose(gs.theta, gs2.theta, precision)
  }

  @Test
  def checkLearn1: Unit = {
    val gs = GammaState(1.0, 3.0)
    checkLearn(gs)
  }
}