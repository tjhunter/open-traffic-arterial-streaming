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

class GammaDirichletScalingTest extends FunSuite with MMLogging with TestingMath {

  @Test
  def test1:Unit = {
    val gss = Array(
      GammaState(5,3),
      GammaState(3,4),
      GammaState(2,2)
    )
    val t = 10.0
    val num_samples = 1000
    val alphas = Array(1.0,1.0,1.0)
    val gm = GammaMixture.positiveCombinationOfGammas(gss, alphas)
    val ll = gm.logPdf(t)
    
    val ts = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_samples)
    val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas,t)
    val ll_est = ecll+entropy
    
    logInfo("ll="+ll)
    logInfo("ll_est="+ll_est)
    logInfo("diff="+(ll_est-ll))
    assertClose(ll, ll_est, 1e-2)
  }

  @Test
  def test2:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,.5),
      GammaState(3,.5)
    )
    val t = 3.0
    val num_samples = 1000
    val alphas = Array(1.0,1.0,1.0)
    val gm = GammaMixture.positiveCombinationOfGammas(gss, alphas)
    val ll = gm.logPdf(t)
    
    val ts = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_samples)
    val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas,t)
    val ll_est = ecll+entropy
    
    logInfo("ll="+ll)
    logInfo("ll_est="+ll_est)
    logInfo("diff="+(ll_est-ll))
    assertClose(ll, ll_est, 1e-2)
  }

  @Test
  def test3:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,.5),
      GammaState(3,.5)
    )
    val t = 3.0
    val num_samples = 10000
    val alphas = Array(1.0,1.0,1.0)
    val gm = GammaMixture.positiveCombinationOfGammas(gss, alphas)
        val u = alphas.map(m.log _).sum /alphas.size
    val ll = gm.logPdf(t) + u
    
    val ts = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_samples)
    val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas,t)
    val ll_est = ecll+entropy
    
    logInfo("ll="+ll)
    logInfo("ll_est="+ll_est)
    logInfo("diff="+(ll_est-ll))
    assertClose(ll, ll_est, 1e-2)
  }
  
  /**
   * Change the coefficient weighting so that it corresponds to a dirichlet distribution when
   * rescaled.
   */
  def testScaledDirichlet(gss:Array[GammaState],t:Double,u:Double,num_samples:Int = 1000):Unit = {
    val n = gss.size
    logInfo("gss: "+gss.mkString(","))
    
    // Simple mixture
    val gm = GammaMixture.sumOfGamma(gss)
    val ll1 = gm.logPdf(t)
    
    // Arbitrary scaling
    val alphas = Array.fill(n)(1.0/u)
    logInfo("alphas: "+alphas.mkString(" "))
    val t_tilde = t / u
    logInfo("t_tilde "+t_tilde)
    
    // Sum of scaled elements
    val ll2 = {
        val scaled_gss2 = (gss.zip(alphas)).map({case (gs,alpha) => gs.scaled(alpha)})
        logInfo("scaled_gss2 "+scaled_gss2.mkString(" "))
        val gm2 = GammaMixture.sumOfGamma(scaled_gss2)
        val u = alphas.map(m.log _).sum /alphas.size
        logInfo("ll2 %f %f" format (gm2.logPdf(t_tilde), u))
        gm2.logPdf(t_tilde) + u
    }
    // Sum of scaled elements by sampling
    val ts = GammaDirichlet.sampleFromScaled(gss, t_tilde, alphas, num_samples)
    val ll3 = {
      val alphas3 = Array.fill(n)(1.0 / t)
      val t3 = 1.0
      val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas3,t3)
      ecll + entropy
    }
    
    val ll4 = {
      val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas,t_tilde)
      ecll + entropy
    }
    
    logInfo("ll1 = "+ll1)
    logInfo("ll2 = "+ll2)
    logInfo("ll3 = "+ll3)
    logInfo("ll4 = "+ll4)
    assertClose(ll1, ll2, 1e-2)
    assertClose(ll1, ll3, 1e-2)
    assertClose(ll1, ll4, 1e-2)
  }


  /**
   * Change the coefficient weighting so that it corresponds to a dirichlet distribution when
   * rescaled.
   */
  def testScaledFull(gss:Array[GammaState],t:Double,alphas:Array[Double],num_samples:Int = 1000):Unit = {
    val n = gss.size
    logInfo("gss: "+gss.mkString(","))
    
    // Sum of scaled elements
    val ll2 = {
        val scaled_gss2 = (gss.zip(alphas)).map({case (gs,alpha) => gs.scaled(alpha)})
        logInfo("scaled_gss2 "+scaled_gss2.mkString(" "))
        val gm2 = GammaMixture.sumOfGamma(scaled_gss2)
        val u = alphas.map(m.log _).sum /alphas.size
        logInfo("ll2 %f %f" format (gm2.logPdf(t), u))
        gm2.logPdf(t) + u
    }
    
    // Sum of scaled elements by sampling
    val ts = GammaDirichlet.sampleFromScaled(gss, t, alphas, num_samples)
    val ll4 = {
      val (ecll, entropy) = GammaDirichlet.ECLLEntropy(ts,gss,alphas,t)
      ecll + entropy
    }
    
    logInfo("ll2 = "+ll2)
    logInfo("ll4 = "+ll4)
    assertClose(ll4, ll2, 1e-2)
  }
  
  @Test
  def test4:Unit = {
    val gss = Array(
      GammaState(2,1),
      GammaState(3,2)
    )
    val u = 2.0
    val t = 5.0
    testScaledDirichlet(gss,t,u)
  }

  @Test
  def test5:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,1),
      GammaState(3,2)
    )
    val u = 3.0
    val t = 5.0
    testScaledDirichlet(gss,t,u)
  }

  @Test
  def test6:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,1),
      GammaState(3,2)
    )
    val alphas = Array(1.0,1.0,1.0)
    val t = 5.0
    testScaledFull(gss,t,alphas)
  }

  @Test
  def test7:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,1),
      GammaState(3,2)
    )
    val alphas = Array(2.0,2.0,2.0)
    val t = 5.0
    testScaledFull(gss,t,alphas)
  }

  @Test
  def test8:Unit = {
    val gss = Array(
      GammaState(1,.5),
      GammaState(2,1),
      GammaState(3,2)
    )
    val alphas = Array(1.0,3.0,1.0)
    val t = 5.0
    testScaledFull(gss,t,alphas)
  }  
}