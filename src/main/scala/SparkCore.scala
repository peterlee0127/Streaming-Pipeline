/* SimpleApp.scala */


import org.apache.log4j._
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}

object SparkCore {
  def setLogger() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.log4j").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("com.github.fommil.netlib").setLevel(Level.OFF)
  }
  def main(args: Array[String]) = {
    setLogger()
//    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark")
//    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
    val spark = SparkSession
      .builder().master("local[4]").appName("Spark").getOrCreate()

//    val travel = spark.read.textFile("./../News-Tool/data/travel/").toDF()
//    val money = spark.read.textFile("./../News-Tool/data/money/").toString()
//    travel.printSchema()

    val sentenceData = spark.createDataFrame(Seq(
      (0.0,"orange black color white yellow kuro brown pink blue green"),
      (1.0,"pc mac iphone watch notebook desktop-pc phone android"),
      (2.0,"hadoop spark big-data data flink apache open-source"),
      (3.0,"taiwan china hong-kong japan usa uk us america korea"),
      (4.0,"dog cat man monkey pig cow sheep bug")
    )).toDF("label", "sentence")

    val trainingData = Forest.transformData(sentenceData)

    val model  = Forest.trainModel(trainingData)

    val test = spark.createDataFrame(Seq(
      (0.0, "dog")
    )).toDF("label", "sentence")

    val testData = Forest.transformData(test)
    val prediction = model.transform(testData)
    prediction.select("probability","prediction").show(false)

  }

}
