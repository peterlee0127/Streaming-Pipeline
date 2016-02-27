/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.sql
import org.apache.log4j._

object SparkCore {
  def setLogger() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.log4j").setLevel(Level.OFF)
    Logger.getLogger("kafka.utils").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("com.github.fommil.netlib").setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    setLogger()
    val conf = new SparkConf().setAppName("SparkCore")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val logFile = "hdfs://hdfs:9000/aizu/dayTweets.json" // Should be some file on your system

    val df = sqlContext.read.json(logFile)
    // df.printSchema()
    df.groupBy("lang").count().show()

    // people.registerTempTable("tweet")
    // val teenagers = sqlContext.sql("SELECT l FROM tweet WHERE age >= 13 AND age <= 19")

  }
}
