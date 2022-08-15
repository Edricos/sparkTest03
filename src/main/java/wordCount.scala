import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("wordCount")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val lines = sc.parallelize(Array("ok am is are", "am is query bassic","bassic is nb"))
    val clips = lines.flatMap(_.split(" "))
    val toMap = clips.map((_, 1))
    val reduced:RDD[(String, Int)] = toMap.reduceByKey(_+_)
    val sorted:RDD[(String, Int)] = reduced.sortBy(_._2, false)
    val conbiner = sorted.repartition(1)
    conbiner.foreach(println)
  }
}
