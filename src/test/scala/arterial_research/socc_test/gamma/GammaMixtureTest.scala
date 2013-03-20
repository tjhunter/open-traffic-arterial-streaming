package arterial_research.socc_test.gamma


import arterial_research.socc.gamma.GammaMixture;
import arterial_research.socc.gamma.GammaState;
import arterial_research.socc.gamma._

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

//@RunWith(classOf[JUnitRunner])
class GammaMixtureTest extends FunSuite with TestingMath {

  val K = 100
  val epsi = 1e-6
  
	test("basic") { basic }
	
  @Test def basic: Unit = {
    val gammas = Array(GammaState(2.0, 8.0), GammaState(.5, 40.0))
    val mix = GammaMixture.sumOfGamma(gammas, K)
    // Values obtained from the python reference
    val xs = Array(1.0, 5.0, 10.0, 20.0, 30.0)
    val ys = Array(-6.39277507, -4.39287651, -3.85918207, -3.78611184, -4.07438066)
    val zs = xs.map(mix.logPdf _)
    for ((y,z) <- ys zip zs) {
      assert(abs(y-z)<epsi, (ys, zs))
    }
  }
  
  def checkDifferentMixtures(gss:Array[GammaState]):Unit = {
    val s1 = GammaMixture.sumOfGammaBreeze(gss)
    val s2 = GammaMixture.sumOfGammaNoBreeze(gss)
    assertEquals(s1.states.size, s2.states.size)
    for ((a,b) <- s1.coefficients zip s2.coefficients) {
      assertClose(a,b,1e-6)
    }
  }
  
  @Test
  def sum1:Unit = {
    val gammas = Array(GammaState(2.0, 8.0), GammaState(.5, 40.0))
    checkDifferentMixtures(gammas)
  }

}

