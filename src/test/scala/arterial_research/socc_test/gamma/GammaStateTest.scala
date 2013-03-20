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


class GammaStateTest extends FunSuite with MMLogging with TestingMath {

  val epsi = 1e-4
  
  @Test
  def state1: Unit = {
    val gs = GammaState(3.0, 50.0)
    val ss = gs.sufficientStatistics
    val gs2 = GammaLearn.mle(ss)
    logInfo(""+gs)
    logInfo(""+gs2)
    assert(gs.isApprox(gs2)(epsi))
  }
  
  @Test
  def state2: Unit = {
    val gs = GammaState(5.4343, 8.43566)
    assert(GammaState.fromMeanVariance(gs.mean, gs.variance).isApprox(gs)(epsi))
  }
  
  @Test
  def ll1: Unit = {
    val gs = GammaState(3.0,1.0)
    val x = 1.0
    val y_expected = -1.6931471805599454
    val y = GammaFunctions.logLikelihood(gs, x)
    assertClose(y_expected, y)
  }
  
}