package arterial_research.socc_test.io


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
import arterial_research.socc.io.DataSliceIndex
import org.joda.time.LocalDate

class DataSliceIndexTest extends FunSuite with MMLogging {

  @Test
  def dsi: Unit = {
    val d = new LocalDate
    val ds = DataSliceIndex("feed", -1, d, "net", 0)
    val d2 = new LocalDate
    val ds2 = DataSliceIndex("feed", -1, d2, "net", 0)
    assert(ds <= ds2)
  }
}