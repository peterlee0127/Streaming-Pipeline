/* SimpleApp.scala */
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.spark.sql._
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

    val spark = SparkSession
    .builder()
    .appName("Spark SQL Twitter")
    .getOrCreate()

    val numClusters = 10
    val numIterations = 10
    val outputModelDir = "./"
    
    val remover = new StopWordsRemover().setInputCol("text").setOutputCol("text")

    val path = "file:///home/node/tw.json"
    val df = spark.read.json(path)
    df.printSchema()
    df.registerTempTable("tweets")
    val texts = df.select("text")//.map(_.head.toString)
    text = remover.transform(text)
    
    
/*
    val vectors = texts.map(Utils.featurize).cache()
    vectors.count() 
    val model = KMeans.train(vectors, numClusters, numIterations)
    sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)

    val some_tweets = texts.take(1000)
    println("----Example tweets from the clusters")
    for (i <- 0 until numClusters) {
      println(s"\nCLUSTER $i:")
        some_tweets.foreach { t =>
          if (model.predict(Utils.featurize(t)) == i) {
            println(t)
          }
        }
    }
  */
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
    //    hashtags.foreach(tag=>
    //        tag.foreach(_.rdd.map(word => (word(0), 1)).reduceByKey(_+_).foreach(println))
    //    )
    //    hashtags.collect().flatMap(word => (word(0), 1)).reduceByKey(_+_).foreach(println)
    // .toJavaRDD.map(word => (word, 1)).reduceByKey(_+_).foreach(println)
    // .collect(
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


  }
}
