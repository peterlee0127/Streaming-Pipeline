/**
  * Created by Peter on 9/8/2016 AD.
  */


import org.apache.log4j._
import org.apache.spark.ml.{Pipeline, PredictionModel, Predictor}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


object Forest {

  def transformData(data:DataFrame): DataFrame = {

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("token")
    val wordsData = tokenizer.transform(data)

    val remover = new StopWordsRemover().setInputCol("token").setOutputCol("filtered")
    val cleanWords = remover.transform(wordsData)

    val hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(cleanWords)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val resultData = idfModel.transform(featurizedData)

    return resultData
/*
      val transformers = Array(
      new StringIndexer().setInputCol("text").setOutputCol("label"),
      new Tokenizer().setInputCol("text").setOutputCol("tokens"),
      new CountVectorizer().setInputCol("tokens").setOutputCol("features")
    )

val rf = new RandomForestClassifier()
.setLabelCol("label")
.setFeaturesCol("features")

val model = new Pipeline().setStages(transformers :+ rf).fit(sentenceData)
val prediction = model.transform(testData)
    prediction.select("tokens").show(false)
    prediction.select("probability","prediction").show(false)

    */

    //    return resultData
  }
  def trainModel(trainData:DataFrame):RandomForestClassificationModel =  {

    val forest = new RandomForestClassifier()
      .setFeatureSubsetStrategy("auto")
//      .setSeed(5043)
    val model = forest.fit(trainData)

    return model
  }
}
