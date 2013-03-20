package arterial_research.socc_test.gamma

import arterial_research.socc.gamma._
import org.junit._
import org.junit.Assert._
import scala.{math=>m}
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

class GammaDirichletTest_ extends FunSuite with MMLogging with TestingMath {
  
  def checkNormalization(gss:Array[GammaState]):Unit = {
    val gm = GammaMixture.sumOfGamma(gss)
    val ks = gss.map(_.k)
    val thetas = gss.map(_.theta)
    
    val n1 = m.exp(gm.logPdf(1.0))
    
    val n2 = GammaDirichlet.normalizationFactor(ks, thetas)
    
    logInfo("n1="+n1)
    logInfo("n2="+n2)
    assertClose(n1,n2)
  }
  
  
  /**
   * Check normalization factors for a dirichlet distribution.
   */
  def checkNormalizationDirichlet(ks:Array[Double], num_samples:Int):Unit = {
    logInfo("======checkNormalizationDirichlet=======")
    val tol = 1e-1
    
    val value = GammaFunctions.volumeStandardSimplex(ks.size-1)
    val thetas = Array.fill(ks.size)(1.0)
    
    val gss = ks.map(GammaState(_, 1.0))
    val gd = new GammaDirichlet(gss)
    val xs0 = gd.sample(num_samples)
    val ps = gd.ll(xs0).map(m.exp _)
    val est1 = ps.map(1.0 / _).sum / num_samples
    val secure_lnorm = GammaDirichlet.normalizationFactorSecure(ks, thetas)
    
//    logInfo("ll0:"+gd.ll(xs0).mkString(" "))
    logInfo("logNorm:"+gd.logNorm)
    logInfo("robust logNorm:"+secure_lnorm)
    assertClose(gd.logNorm, secure_lnorm,tol)
    
    // Compute the means of the samples:
    val mean = {
      val m = DenseVector.zeros[Double](ks.size)
      for (x <- xs0) {
        m += DenseVector(x)
      }
      m / num_samples.toDouble
    }
    val expected_mean = DenseVector(ks) / ks.sum
    logInfo("Expected:"+expected_mean)
    logInfo("Got:"+mean)
    
    val xs2 = Dirichlet.sym(1.0, ks.size).sample(num_samples).map(_.data).toArray
    val ps2 = gd.ll(xs2).map(m.exp _)
    val est2 = 1.0 / (ps2.sum / num_samples)
    
    val xs3 = Dirichlet(ks).sample(num_samples).map(_.data).toArray
    val ps3 = gd.ll(xs3).map(m.exp _)
    val est3 = ps3.map(1.0 / _).sum / num_samples
    
    logInfo("target="+value)
    logInfo("est1="+est1)
    logInfo("est2="+est2)
    logInfo("est3="+est3)
    assertClose(value, est1,tol)
    assertClose(value, est2,tol)
    assertClose(value, est3,tol)
  }
  
  def checkNormalizationSampling(gss:Array[GammaState], num_samples:Int):Unit = {
    logInfo("======checkNormalizationSampling=======")
    val tol = 1e-1
    val value = GammaFunctions.volumeStandardSimplex(gss.size-1) 
    val gd = new GammaDirichlet(gss)
    val xs0 = gd.sample(num_samples)
    val ps = gd.ll(xs0).map(m.exp _)
    val est1 = ps.map(1.0 / _).sum / num_samples
    
    val xs2 = Dirichlet.sym(1.0, gss.size).sample(num_samples).map(_.data).toArray
    val ps2 = gd.ll(xs2).map(m.exp _)
    val est2 = 1.0 / (ps2.sum / num_samples)
    
    logInfo("target="+value)
    logInfo("est1="+est1)
    logInfo("est2="+est2)

    val mean = {
      val m = DenseVector.zeros[Double](gss.size)
      for (x <- xs0) {
        m += DenseVector(x)
      }
      m / num_samples.toDouble
    }
//    val expected_mean = DenseVector(gss.map(_.k)) / gss.map(_.k).sum
//    logInfo("Expected mean:"+expected_mean)
    logInfo("Got mean:"+mean)
    assertClose(value, est1,tol)
    assertClose(value, est2,tol)    

  
    val secure_lnorm = GammaDirichlet.normalizationFactorSecure(gss.map(_.k), gss.map(_.theta))
    logInfo("logNorm:"+gd.logNorm)
    logInfo("robust logNorm:"+secure_lnorm)
    assertClose(gd.logNorm, secure_lnorm,tol)
  }
  
  @Test
  def test1: Unit = {
    val gss = Array(GammaState(4, 1),GammaState(3, 1),GammaState(2, 1))
    checkNormalization(gss)
  }
  
  @Test
  def test2: Unit = {
    val ks = Array(4.0,3.0,2.0)
    checkNormalizationDirichlet(ks, 100000)
  }

  @Test
  def test2_1: Unit = {
    val ks = Array(4.0,3.0)
    checkNormalizationDirichlet(ks, 100000)
  }
  
  @Test
  def test2_2: Unit = {
    val ks = Array(4.0,3.0,2.0,1.0)
    checkNormalizationDirichlet(ks, 100000)
  }
  
  // This is a test for the scaled Dirichlet
//  @Test
//  def test3: Unit = {
//    val gss = Array(
//        GammaState(2, 1.0/2),
//        GammaState(3, 1.1/3),
//        GammaState(2, 1.2/2))
//    checkNormalizationSampling(gss, 100000)
//  }

  @Test
  def test4: Unit = {
    val gss = Array(
        GammaState(4, .5),
        GammaState(2, .5))
    checkNormalizationSampling(gss, 100000)
  }

  @Test
  def test5: Unit = {
    val gss = Array(
        GammaState(4, .5),
        GammaState(1, .5),
        GammaState(3, .5),
        GammaState(2, .5))
    checkNormalizationSampling(gss, 100000)
  }
  
  // This is a test for the scaled Dirichlet
//  @Test
//  def test6: Unit = {
//    val gss = Array(
//        GammaState(4, .5),
//        GammaState(2, 1.0))
//    checkNormalizationSampling(gss, 100000)
//  }
}

class GammaDirichletNormTest extends FunSuite with MMLogging with TestingMath {
  def checkNorm(gss:Array[GammaState]):Unit = {
    val ks = gss.map(_.k)
    val thetas = gss.map(_.theta)
    val secure_lnorm = GammaDirichlet.normalizationFactorSecure(ks, thetas)
    val norm = GammaDirichlet.normalizationFactor(ks,thetas)
    val lnorm = math.log(norm)
    
//    logInfo("ll0:"+gd.ll(xs0).mkString(" "))
    logInfo("logNorm:"+lnorm)
    logInfo("robust logNorm:"+secure_lnorm)
    logInfo("diff:"+(secure_lnorm-lnorm))
    assertClose(lnorm, secure_lnorm,1e-5)
    
  }
  @Test
  def test6: Unit = {
    val gss = Array(
        GammaState(4, .5),
        GammaState(2, 1.0))
    checkNorm(gss)
  }
  
}