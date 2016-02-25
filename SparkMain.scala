/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
//    val logData = sc.textFile(logFile, 2).cache()
    val files = sc.addFile("https://raw.githubusercontent.com/peterlee0127/iBeacon_AutoCheckIn/master/README.md")
    val logData = sc.textFile("README.md")
    val numAs = logData.filter(line => line.contains("iOS")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with iOS: %s, Lines with b: %s".format(numAs, numBs))
  }
}
