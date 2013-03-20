package arterial_research.socc_test

import arterial_research.socc._
import org.junit._
import org.junit.Assert._
import math._
import java.io.File
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import breeze.stats.distributions.Gamma
import arterial_research.socc.gamma.GammaLearn

class OptimizerTest extends FunSuite {

//  test("breeze") { testBreeze }
  
  @Test def testBreeze() {
    val mean = 2.834312
    val meanOfLogs = -0.689661
    val n=5.000000
    val ss = Gamma.SufficientStatistic(n, meanOfLogs, mean)
    val gs = GammaLearn.mle(ss)
  }
}