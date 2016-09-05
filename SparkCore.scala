/* SimpleApp.scala */
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.clustering.KMeans

//import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
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
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val spark = SparkSession
    .builder()
    .appName("Spark")
    .getOrCreate()

    val news = sqlContext.load("./../News-Tool/data/test")
    
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(news)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData).select("features").rdd//.map{_.toSeq}
    rescaledData.collect().take(20).foreach(println)


  }
}
