package travel_time

import breeze.stats.distributions.ContinuousDistr
import breeze.stats.distributions.Moments


trait ScalaTravelTimeDistribution  extends ContinuousDistr[Double] with HasCumulative[Double] with Moments[Double] {

}