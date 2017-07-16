/* SparkCore.scala
* -Xms500m -Xmx5000m -Xss1m
* */

import java.io.File
import java.util.HashMap

import scala.io.Source._
import scala.util.parsing.json._
import kafka.serializer.StringDecoder
import org.apache.log4j._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.ml.{Pipeline, PipelineModel, PredictionModel, Predictor}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StringIndexer, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_json



object SparkCore {
  var numberOfSample = 1000000
  var numberOfFeature = 1000
  var pathPrefix = "./../news-tool/train/"
  var Class = List("entertainment","health","money","sport","politics")
  //var brokerList = "192.168.4.71:9092,192.168.4.72:9092,192.168.4.73:9092,192.168.4.74:9092,192.168.4.75:9092"
  var brokerList = "192.168.4.73:9092"
  var zookeeperInfo = "192.168.4.70:2181/dcos-service-kafka"
  //var brokerList = "192.168.4.71:9092"
  def setLogger() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("zookeeper").setLevel(Level.OFF)
    Logger.getLogger("kafka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
  }
  def getDirectoryInfo(path:List[String]) = {
    for(i <- 0 until path.length) {
      val count = new File(path(i)).listFiles.length-1
      print(Class(i)+":"+count+", ")
      if(count<numberOfSample) {
        numberOfSample = count
      }
    }
    println("Take "+numberOfSample +" from each domain")
  }
  def getProducer(): KafkaProducer[String, String] = {
    val prouderProps = new HashMap[String, Object]()
    prouderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    prouderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prouderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prouderProps)
    return producer
  }
  def train(trainingSet:DataFrame,testSet:DataFrame) : CrossValidatorModel = {
    var regex = new RegexTokenizer().setInputCol("sentence").setOutputCol("filtered").setPattern("\\w+").setGaps(false)
    var hashingTF = new HashingTF().setInputCol("filtered").setOutputCol("features").setNumFeatures(numberOfFeature)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val countVector = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(100)

    var forest = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setFeatureSubsetStrategy("auto")

    val desTree = new DecisionTreeClassifier()
    val naiveBayes = new NaiveBayes().setModelType("multinomial")

    val transformers = Array(
      //new StringIndexer().setInputCol("group").setOutputCol("label"),
      //new Tokenizer().setInputCol("sentence").setOutputCol("tokens"),
      //new StopWordsRemover().setStopWords(getStopWords).setCaseSensitive(false).setInputCol("tokens").setOutputCol("filtered"),
      regex,hashingTF,naiveBayes
    )

      val pipeLine = new Pipeline().setStages(transformers)

// We use a ParamGridBuilder to construct a grid of parameters to search over.
// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(5000, 10000, 15000))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeLine)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val model = cv.fit(trainingSet)

    val bestModel = model.bestModel.asInstanceOf[PipelineModel]
    val stages = bestModel.stages

    val hashingStage = stages(1).asInstanceOf[HashingTF]
    println("numFeatures = " + hashingStage.getNumFeatures)


    val prediction = model.transform(testSet)
    evaluationMetrics(prediction)

    //    val da = prediction.select("label","prediction","filtered")
    //    da.write.format("json").mode("OverWrite").save("prediction")

        return model
  }
  def evaluationMetrics(prediction:DataFrame) {
    /*
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    
    val EvAccuracy = evaluator.evaluate(prediction)
    println("Accuracy: "+EvAccuracy)
    */
    val labelAndPrediction = prediction.select("label","prediction").rdd.map( row => 
            (row.getAs[Double]("label"),
           row.getAs[Double]("prediction")
    ))
    val metrics = new MulticlassMetrics(labelAndPrediction)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
    
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

   
  }
  def main(args: Array[String]) = {
    setLogger()
    val spark = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.executor.uri","http://192.168.4.69:8888/spark.tar.gz")
      .config("spark.cores.max","30")
      .appName("Twitter").getOrCreate()
    import spark.implicits._
/*    
    val configuration = scala.io.Source.fromFile("./configuration.config").getLines().mkString
    val json = JSON.parseFull(configuration)
    val map : Map[String, Option[Any]] = json.get.asInstanceOf[Map[String, Option[Any]]];
    var pathPrefix = map.get("path").get.asInstanceOf[String]
    Class = map.get("Class").get.asInstanceOf[List[String]]
    numberOfSample = map.get("numberOfSample").get.asInstanceOf[Double].toInt
    numberOfFeature = map.get("numberOfFeature").get.asInstanceOf[Double].toInt
    */
    numberOfSample = 10000

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))


    val kafkaParams = Map[String, String](
        "zookeeper.connect"->zookeeperInfo,
        "bootstrap.servers" -> brokerList,
        "group.id"->"spark",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> "false"
        )
    //val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("tweet"))
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,   Subscribe[String, String](Array("tweet"),kafkaParams))
/*
   var path = Class.map(pathPrefix + _)
   getDirectoryInfo(path)
   val trainClass = path(0)
      var dataframe = spark.read.textFile(trainClass).toDF("sentence").withColumn("label", when($"sentence".isNotNull, 0.0))
      var df = dataframe.limit(numberOfSample)
      var Array(trainingSet, testSet) = df.randomSplit(Array(0.8, 0.2))
      for (i <- 1 until path.length) {
          val labeled = i.toDouble
          var classDF = spark.read.textFile(path(i)).toDF("sentence").withColumn("label", when($"sentence".isNotNull, labeled))
          var dataDF = classDF.limit(numberOfSample)
          val Array(training, test) = dataDF.randomSplit(Array(0.8, 0.2))
          trainingSet = trainingSet.union(training)
          testSet = testSet.union(test)
      }
  
  println("training:"+trainingSet.count()+"  testing:"+testSet.count())
    val model = train(trainingSet ,testSet)
//    SVM.train(trainingSet ,testSet, ssc)
    //val modelPath = "./model"
  */
    val modelPath = "hdfs://192.168.4.75:9001/model"
  //  model.write.overwrite().save(modelPath)

    val model = CrossValidatorModel.load(modelPath)
/*
    val twitterData = Twitter.twitterData
    var twitterDF = spark.createDataFrame(twitterData).toDF("label", "sentence")
    var twitterTest = twitterDF.withColumn("label", twitterDF.col("label").cast(DoubleType))
    val twitterPre = model.transform(twitterTest)
    evaluationMetrics(twitterPre)
*/


 println("\nStart Reading Stream Text")
    
    val prouderProps = new HashMap[String, Object]()
    prouderProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    prouderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prouderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")


    kafkaStream.map(_.value).foreachRDD {
        rdd =>
            val streamDF = spark.read.json(rdd)//.withColumn("label", when($"sentence".isNotNull, 0.0))
            if(streamDF.columns.contains("sentence")){
            val prediction = model.transform(streamDF).select("prediction","id","original")
   
   prediction.toJSON.foreachPartition((partisions: Iterator[String]) => {
                  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prouderProps)
                partisions.foreach((line: String) => {
                  try {
                  producer.send(new ProducerRecord[String, String]("result", line))
                  } catch {
                  case ex: Exception => {
                    println(ex)
                  }
                 
                 }
                  })
         
         })//prediction
           
          }//contains("sentence")
   } 
  
  ssc.start()
    ssc.awaitTermination()
  }

}
