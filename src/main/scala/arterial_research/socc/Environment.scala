package arterial_research.socc
import core_extensions.MMLogging

/**
 * Contains all the environment variables required by the program
 */
object Environment extends MMLogging {

  @transient private val props = new sys.SystemProperties
  
  /**
   * Reads mesos.hostname
   */
  def mesosHostname:Option[String] = {
    props.get("mesos.hostname")
  }
  
  /**
   * Reads socc.hdfs.root
   */
  def hdfsRoot:Option[String] = {
    props.get("socc.hdfs.root")
  }
  
  /**
   * The number of compute nodes.
   * Reads mesos.nodes.count
   */
  def numberNodes:Option[Int] = {
    props.get("mesos.nodes.count").map(_.toInt)
  }
  
}