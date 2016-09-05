import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF

object Kmeans {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "reuter-demo") // --(1)

  }
}
