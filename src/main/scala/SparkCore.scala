/* SimpleApp.scala */

import org.apache.spark.sql.functions._ // for `when`
import org.apache.log4j._
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer, StringIndexer}
import org.apache.spark.ml.Pipeline

object SparkCore {
  def setLogger() = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.log4j").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("com.github.fommil.netlib").setLevel(Level.OFF)
  }
  def main(args: Array[String]) = {
    setLogger()
    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession
      .builder().master("local[4]").appName("Spark").getOrCreate()

    val travel = spark.read.textFile("./../News-Tool/data/travel/").toDF("sentence")
    val travelDF = travel.withColumn("label",  when($"sentence".isNotNull, 0.0))

    val money = spark.read.textFile("./../News-Tool/data/money/").toDF("sentence")
    val moneyDF = money.withColumn("label",  when($"sentence".isNotNull, 1.0))
    println("travel:"+travel.count()+"\nmoney:"+money.count())


    val sentenceData = travelDF.union(moneyDF)
    val Array(trainingSet,testSet) = sentenceData.randomSplit(Array(0.8,0.2))

//    val trainedData = Forest.transformData(trainingSet)
//    val model  = Forest.trainModel(trainedData)
//    model.write.overwrite().save("model")
//    val model = RandomForestClassificationModel.load("model")

    val transformers = Array(
//      new StringIndexer().setInputCol("group").setOutputCol("label"),
      new Tokenizer().setInputCol("sentence").setOutputCol("tokens"),
      new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered"),
      new CountVectorizer().setInputCol("filtered").setOutputCol("features")
    )
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val model = new Pipeline().setStages(transformers :+ rf).fit(trainingSet)
    val prediction = model.transform(testSet)

//    val testedData = Forest.transformData(testSet)
//    val prediction = model.transform(testedData)
    prediction.select("probability","label","prediction").show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(prediction)
    println("Test Error = " + (1.0 - accuracy))

//    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)
  }

}
