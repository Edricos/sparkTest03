import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

//import scala.WordCount.remote_file

object test01 {
  private val master = "spark://a1:7077"
  private val remote_file = "hdfs://a1:9000/test/wc.txt"

  def main(args: Array[String]) {
    //in Scala
    //    SparkSession.builder.getOrCreate().sparkContext.setLogLevel("ERROR")


    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")
    val conf = new SparkConf()
      .setAppName("test01")
      .setMaster("local")

    val sc = new SparkContext(conf)
    //    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //    rdd1.foreach(println)
    sc.setLogLevel("ERROR")


    val trainingDataFile = "C:\\Users\\edric\\IdeaProjects\\sparkTest03\\src\\main\\resources\\iris.txt"
    val testDataFile = "C:\\Users\\edric\\IdeaProjects\\sparkTest03\\src\\main\\resources\\iris.txt"
    val featureNumber = 4
    val splitChar = " "
    val K = 7

    var source = Source.fromFile(trainingDataFile, "UTF-8")
    var lines = source.getLines().toArray
    source.close()

    val feature_list: Array[ListBuffer[Double]] = new Array[ListBuffer[Double]](featureNumber)
    for (i <- 0 until featureNumber) {
      feature_list(i) = new ListBuffer[Double]
    }

    for (i <- 0 until lines.length) {
      val featureArray = lines(i).trim.split(splitChar)
      for (j <- 0 until featureNumber) {
        feature_list(j).append(featureArray(j).toDouble)
      }
    }

    val minFeatureVal: Array[Double] = new Array[Double](featureNumber)
    val maxFeatureVal: Array[Double] = new Array[Double](featureNumber)
    for (i <- 0 until featureNumber) {
      maxFeatureVal(i) = feature_list(i).max
      minFeatureVal(i) = feature_list(i).min
    }

    val linesRDD: RDD[String] = sc.textFile(trainingDataFile)

    val featuresAndClassRDD: RDD[(Array[Double], String)] = linesRDD.map(x => {
      val featureArray: Array[Double] = new Array[Double](featureNumber)
      for (i <- 0 until featureNumber) {
        featureArray(i) = x.split(splitChar)(i).toDouble
      }
      val classLabel = x.split(splitChar)(featureNumber)
      (featureArray, classLabel)
    })

    val normFeatureAndClassRDD: RDD[(Array[Double], String)] = featuresAndClassRDD.map(x => {
      val normFeatureArray: Array[Double] = new Array[Double](featureNumber)
      for (i <- 0 until featureNumber) {
        normFeatureArray(i) = (x._1(i) - minFeatureVal(i)) / (maxFeatureVal(i) - minFeatureVal(i))
      }
      (normFeatureArray, x._2)
    })

    source = Source.fromFile(testDataFile, "UTF-8")
    lines = source.getLines().toArray
    source.close()

    var errorCount = 0

    for (i <- 0 until lines.length) {
      val featureAndClassArray = lines(i).trim.split(splitChar)
      val featureArray: Array[Double] = new Array[Double](featureNumber)
      for (i <- 0 until featureNumber) {
        featureArray(i) = featureAndClassArray(i).toDouble
      }
      val classLabel = featureAndClassArray(featureNumber)

      val normFeatureArray: Array[Double] = new Array[Double](featureNumber)
      for (i <- 0 until featureNumber) {
        normFeatureArray(i) = (featureArray(i) - minFeatureVal(i)) / (maxFeatureVal(i) - minFeatureVal(i))
      }

      val distAndClassRDD: RDD[(Double, String)] = normFeatureAndClassRDD.map(x => {
        var sum: Double = 0
        for (i <- 0 until featureNumber) {
          sum = sum + (normFeatureArray(i) - x._1(i)) * (normFeatureArray(i) - x._1(i))
        }
        val dist = math.sqrt(sum)
        (dist, x._2)
      })

      val distAndClassSortRDD: RDD[(Double, String)] = distAndClassRDD.sortByKey()
      val nearestNeighborArray: Array[(Double, String)] = distAndClassSortRDD.take(K)
      val nearestNeighborRDD = sc.parallelize(nearestNeighborArray)
      println(i + "-------------------------------")
      nearestNeighborRDD.repartition(1).foreach(x => println(x))

      val nearestClassRDD: RDD[String] = nearestNeighborRDD.map(x => x._2)
      val classMapRDD: RDD[(String, Int)] = nearestClassRDD.map(x => (x, 1))
      val classCountRDD: RDD[(String, Int)] = classMapRDD.reduceByKey(_ + _)
      val classCountSortRDD: RDD[(String, Int)] = classCountRDD.sortBy(_._2, false)
      println("--------------------------------")
      classCountSortRDD.repartition(1).foreach(x => println(x))

      val classifyResult: Array[(String, Int)] = classCountSortRDD.take(1)
      println("--------------------------------")
      println("预测分类结果：" + classifyResult(0)._1)
      println("真实分类结果：" + classLabel)

      if (classLabel != classifyResult(0)._1) {
        errorCount = errorCount + 1
        println("***预测错误记录编号：" + i)
      }
    }

    println("--------------------------------")
    println("预测错误次数：" + errorCount)
    println("错误率：" + 100 * errorCount.toDouble / lines.length + "%")

  }
}
