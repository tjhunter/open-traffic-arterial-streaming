package arterial_research.socc.testing

import netconfig.Link
import travel_time.ScalaTravelTimeDistribution

/**
 * A number of statistics provided when estimating a distribution against a sample.
 */
case class ErrorStatistics(
  val ttMean: Double,
  val ttStdDev: Double,
  val ttObs: Double,
  val logPdf: Double,
  val percentile: Double) {
}

object ErrorStatistics {
  def getErrorStat(ttd: ScalaTravelTimeDistribution, tt_obs: Double): ErrorStatistics = {
    val tt_mean = ttd.mean
    val tt_std_dev = math.sqrt(ttd.variance)
    val log_pdf = ttd.logPdf(tt_obs)
    val percentile = getPercentile(ttd, tt_obs)
    ErrorStatistics(tt_mean, tt_std_dev, tt_obs, log_pdf, percentile)
  }

  def getPercentile(ttd: ScalaTravelTimeDistribution, tt_obs: Double): Double = {
    ttd.cdf(tt_obs)
  }
}