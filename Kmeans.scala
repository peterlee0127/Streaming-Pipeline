import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

object Kmeans {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "reuter-demo") // --(1)

    val stopwords = List(
      "a", "an", "the", "who", "what", "are", "is", "was", "he", "she", "of",
      "to", "and", "in", "said", "for", "The", "on", "it", "with", "has", "be",
      "will", "had", "this", "that", "by", "as", "not", "from", "at", "its",
      "they", "were", "would", "have", "or", "we", "his", "him", "her") // --(2)

    val documents = sc
      .textFile("./noFilterTweets.json")
      .map {
        _.split(" ").toSeq.filter { word => !stopwords.contains(word) }
      } // --(3)

    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(documents)

    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf) // --(4)

    val k = 18
    val maxIterations = 50
    val kmeansModel = KMeans.train(tfidf, k, maxIterations) // --(5)

    val docsTable =
      (kmeansModel.predict(tfidf) zip documents).groupBy {
        case (clusterId: Int, _) => clusterId
      }.sortBy {
        case (clusterId: Int, pairs: Iterable[(Int, Seq[String])]) => clusterId
      }.map {
        case (clusterId: Int, pairs: Iterable[(Int, Seq[String])]) => (clusterId, pairs.map {
          _._2
        })
      } // --(6)

    val wordsRanking =
      docsTable.map {
        case (clusterId: Int, docs: Iterable[Seq[String]]) => (clusterId, docs.flatten)
      }.map {
        case (clusterId: Int, words: Iterable[String]) =>
          var wordsMap = Map[String, Int]()
          words.foreach { word =>
            val oldCount = wordsMap.getOrElse(key = word, default = 0)
            wordsMap += (word -> (oldCount + 1))
          }
          (clusterId, wordsMap.toSeq.sortBy {
            _._2
          }(Ordering[Int].reverse))
      }.map {
        case (clusterId: Int, wordsTable: Seq[(String, Int)]) => (clusterId, wordsTable.take(10))
      } // --(7)

    wordsRanking.foreach {
      case (clusterId: Int, wordsTable: Seq[(String, Int)]) =>
        val wordCountString = wordsTable.map {
          case (word, count) => s"$word($count)"
        }.reduceLeft {
          (acc: String, elem: String) => acc + ", " + elem
        }
        println(s"ClusterId : $clusterId, Top words : $wordCountString")
    } // --(8)
  }
}