package arterial_research.socc_test.gamma

import arterial_research.socc.gamma._
import org.junit._
import org.junit.Assert._
import scala.{ math => m }
import java.io.File
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import arterial_research.socc._
import breeze.linalg.DenseVector._
import breeze.linalg.DenseVector
import core_extensions.MMLogging
import breeze.stats.distributions.Dirichlet

class GammaMixtureNormalizationTest extends FunSuite with MMLogging with TestingMath {

  def checkDelta(gss: Array[GammaState], n: Int): Unit = {
    val ks = gss.map(_.k)
    val thetas = gss.map(_.theta)
    val delta1 = GammaMixture.logDelta(ks, thetas, n)
    assertEquals(delta1.size, n)
    val delta2 = GammaMixture.logDeltaApproximate(ks, thetas, n)
    assertEquals(delta2.size, n)
    logInfo("delta1: " + delta1.mkString(" "))
    logInfo("delta2: " + delta2.mkString(" "))
    val delta_max = delta1.max
    for (u <- 0 until n) {
      if (delta1(u) >= delta_max - 50) {
        val d = math.exp(delta1(u)-delta2(u))
        assertClose(1.0, d, 1e-2)
      }
    }
    val log_C = GammaMixture.logC(ks, thetas)
    val n1 = delta1.map(d => math.exp(d + log_C)).sum
    assertClose(n1, 1.0)
    val n2 = delta1.map(d => math.exp(d + log_C)).sum
    assertClose(n2, 1.0)
  }

  def checkZeta(gss: Array[GammaState], zeta: Double, n: Int, dx: Double = 1e-2): Unit = {
    val ks = gss.map(_.k)
    val thetas = gss.map(_.theta)
    val log_deltas = GammaMixture.logDelta(ks, thetas, n)
    val zeta_approx = GammaMixture.zetaApproximation(ks, thetas, log_deltas, dx/5)
    assertClose(zeta_approx, zeta, dx)
  }

  @Test
  def test1: Unit = {
    val gss = Array(
      GammaState(4, .5),
      GammaState(2, 1.0))
    val zeta = 4.0
    checkZeta(gss, zeta, 100)
    checkDelta(gss, 500)
  }

  @Test
  def test2: Unit = {
    val ks = Array(29.900281, 31.325212, 17.427608, 9.014660, 28.530014, 13.008574)
val thetas = Array(0.209001, 0.613586, 0.603777, 0.579253, 0.810003, 0.114251)
    val gss = (ks zip thetas) map {case (k,theta) => GammaState(k, theta)}
    val zeta = 59.138573999999011
    checkZeta(gss, zeta, 1900)
    checkDelta(gss, 3000)
  }
}