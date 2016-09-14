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

    val remover = new StopWordsRemover().setInputCol("token").setOutputCol("filteredToken")
    val cleanWords = remover.transform(wordsData)

    val hashingTF = new HashingTF().setInputCol("filteredToken").setOutputCol("TFrawFeature")
    val featurizedData = hashingTF.transform(cleanWords)

    val idf = new IDF().setInputCol("TFrawFeature").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val resultData = idfModel.transform(featurizedData)

    return resultData
  }
  def trainModel(trainData:DataFrame):RandomForestClassificationModel =  {

    val forest = new RandomForestClassifier()
    val model = forest.fit(trainData)

    return model
  }
}
