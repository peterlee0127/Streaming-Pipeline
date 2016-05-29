/* SimpleApp.scala */
import org.apache.log4j._
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql
import scala.collection.mutable.WrappedArray

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

    //val logFile = "hdfs://hdfs:9000/aizu/dayTweets.json" // Should be some file on your system
    val logFile = "rooms.json"
    val df = sqlContext.read.json(logFile)
    // df.printSchema()
    df.registerTempTable("rooms")
    val result = sqlContext.sql("SELECT payload FROM rooms WHERE payload IS NOT NULL")
    result.persist()
    result.printSchema()

//    val hashtags = result.select("text").map(_.getSeq[Seq[String]](0))
  //  hashtags.map(println("row:"+_))
//    hashtags.foreach(tag=>
//        tag.foreach(_.rdd.map(word => (word(0), 1)).reduceByKey(_+_).foreach(println))
//    )
//    hashtags.collect().flatMap(word => (word(0), 1)).reduceByKey(_+_).foreach(println)
    // .toJavaRDD.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    // .collect()
    // .map(_.getAs[Seq[String]](0)).foreach(
    //     _.foreach(item=>
    //         item.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    //         // item
    //     )
    // )
    // .flatMap(println(_))
        //_.getAs[Seq[String]](0))
    // hashtags.foreach(println(_))
    // val hashtagsArray:WrappedArray[String] = hashtags
    //.getAs[Seq[String]](0)
    //flatMap(word => (word, 1)).reduceByKey(_+_)
    // hashtagsArray.flatMap(item=> println(item))
    // foreach(println)

    // flatMap(word => (word, 1)).reduceByKey(_+_).foreach(println)
    //.count().show(100)//.foreach(println)


    df.groupBy("payload.country").count().sort("count").show(300)
    //df.groupBy("place.country").count().sort("count").show(50)

  }
}
