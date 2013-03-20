package arterial_research.socc.gamma

object numerics {
  def logSum(xs: Array[Double], from: Int, until_ : Int): Double = {
    var max = Double.NegativeInfinity

    {
      var idx = from
      while (idx < until_) {
        val xs_idx = xs(idx)
        if (max < xs_idx) {
          max = xs_idx
        }
        idx += 1
      }
    }
    if (max <= Double.NegativeInfinity) {
      Double.NegativeInfinity
    } else {
      var x = 0.0

      {
        var idx = from
        while (idx < until_) {
          val xs_idx = xs(idx)
          x += math.exp(xs_idx - max)
          idx += 1
        }

      }
      math.log(x) + max
    }
  }

  def logSum(xs: Array[Double], length: Int): Double = logSum(xs, 0, length)

  def logSum(xs: Array[Double]): Double = logSum(xs, 0, xs.length)
}