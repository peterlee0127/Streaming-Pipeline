/* SimpleApp.scala */
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

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
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val spark = SparkSession
    .builder()
      .master("local[4]")
    .appName("Spark")

    val word = sc.textFile("./../News-Tool/data/test/01.txt")
    val numAs = word.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    numAs.collect().foreach(println(_))

    //    val news = sqlContext.load("./../News-Tool/data/test")
    //
//    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//    val wordsData = tokenizer.transform(news)
//    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
//    val featurizedData = hashingTF.transform(wordsData)
//
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData).select("features").rdd//.map{_.toSeq}
//    rescaledData.collect().take(20).foreach(println)


  }
}
