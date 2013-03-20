package arterial_research.socc_test.gamma

trait TestingMath {

  val TOLERANCE = 1e-6
  

  def assertClose(a: Double, b: Double):Unit =
    assertClose(a,b,TOLERANCE)

  def assertClose(a: Double, b: Double,tol:Double):Unit =
    assert(math.abs(a - b) < tol, (a,b,a-b,tol))
}