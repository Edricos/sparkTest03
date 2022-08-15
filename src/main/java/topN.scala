import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object topN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("topN")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val txt:RDD[String] = sc.textFile("C:\\Users\\edric\\IdeaProjects\\sparkTest03\\src\\main\\java\\score.txt", 1)
    val toMap:RDD[(String, Int)] = txt.map(line=>{
      val name = line.split(",")(0)
      val score = line.split(",")(1)
      (name, score.toInt)
    })

    val top3 = toMap.groupByKey().map(gruopedData=>{
      val top3Scores:List[Int] = gruopedData._2.toList.sortWith(_>_).take(3)
      val name:String = gruopedData._1
      (name, top3Scores)
    })
    top3.foreach(scores=>{
      println("name:"+scores._1)
      val scoreList = scores._2.toList
      print("scores:")
      for (elem <- scoreList) {
        print(elem+" ")
      }
      println("")
      println()
    })
  }
}
