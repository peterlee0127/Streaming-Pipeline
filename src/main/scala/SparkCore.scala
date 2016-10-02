/* SimpleApp.scala */

import java.io.File


import org.apache.spark.streaming._
import org.apache.log4j._
import org.apache.spark.ml.{Pipeline, PipelineModel, PredictionModel, Predictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, Tokenizer,RegexTokenizer}

object SparkCore {
  var sampleCount = 1000000
  val pathPrefix = "./../News-Tool/data/"
//  val Class = List("travel","money","entertainment","health","tech","sport","politics")
  val Class = List("entertainment","politics")
  def setLogger() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
  }
  def getDirectoryInfo(path:List[String]) = {
    for(i <- 0 until path.length) {
      val count = new File(path(i)).listFiles.length-1
      println(count+"\t"+Class(i))
      if(count<sampleCount) {
        sampleCount = count
      }
    }
    println("Will take "+sampleCount+" files of each category")

  }
  def main(args: Array[String]) = {
    setLogger()

    val spark = SparkSession
      .builder().master("local[4]").appName("Spark").getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val stream = ssc.socketTextStream("localhost", 9999)

    val path = Class.map(pathPrefix + _)
    val classCount = Class.size
    getDirectoryInfo(path)



    var df = spark.read.textFile(path(0)).toDF("sentence").withColumn("label",  when($"sentence".isNotNull, 1.0)).limit(sampleCount)
    for(i <- 1 until path.length) {
      val labeled = i.toDouble+1.0
      var pa = path(i)
      val dataDF = spark.read.textFile(path(i)).toDF("sentence").withColumn("label",  when($"sentence".isNotNull, labeled)).limit(sampleCount)
      df = df.union(dataDF)
    }

    val Array(trainingSet,testSet) = df.randomSplit(Array(0.7,0.3))
//    val set = spark.read.format("libsvm").load("./../News-Tool/data/out.txt")
//    val Array(trainingSet,testSet) = set.randomSplit(Array(0.7,0.3))



  val transformers = Array(
  //new StringIndexer().setInputCol("group").setOutputCol("label"),
//    new Tokenizer().setInputCol("sentence").setOutputCol("tokens"),
    new RegexTokenizer().setInputCol("sentence").setOutputCol("tokens").setPattern("\\w+").setGaps(false),
    new StopWordsRemover().setCaseSensitive(false).setInputCol("tokens").setOutputCol("filtered"),
//    new CountVectorizer().setInputCol("filtered").setOutputCol("features"),
    new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures").setNumFeatures(100*classCount),
    new IDF().setInputCol("rawFeatures").setOutputCol("features"),
    new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
      .setNumTrees(classCount)
//    .setFeatureSubsetStrategy("auto")
//    .setImpurity("gini")
//    .setMaxDepth(3)
//    .setMaxBins(32)
//    .setSeed(50165)
  )


    val pipeLine = new Pipeline().setStages(transformers)
    val model = pipeLine.fit(trainingSet)

    val rfModel = model.stages(4).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)


    val prediction = model.transform(testSet)

    val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(prediction)
    println("Accuracy: "+accuracy)
    println("Test Error = " + (1.0 - accuracy))

    println("\nStart Reading Stream Text")

    stream.foreachRDD {
        rdd =>
          if(!rdd.isEmpty()) {

            val streamDF = rdd.toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
            model.transform(streamDF).select("probability","prediction").show(false)
            //probability
          }
    }

    ssc.start()
    ssc.awaitTermination()


  }
  def EvaluateModel(data:DataFrame, model:RandomForestClassificationModel): Unit = {

//

  }

}
