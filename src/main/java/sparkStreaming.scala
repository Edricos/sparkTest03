
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object sparkStreaming {
  private val master = "spark://a1:7077"
  private val remote_file = "hdfs://a1:9000/test/wc.txt"

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")
    val conf = new SparkConf()
      .setAppName("sparkStreaming")
    .setMaster("local")
    
    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
//    文件输入
    val lines = ssc.textFileStream("C:\\unzip\\spark-3.0.3-bin-hadoop3.2\\streaming")
//    val words = lines.flatMap(_.split(" "))
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//    wordCounts.print()
    val score = lines.map(x=>{
      val stu_id = x.split(" ")(0).toInt
      val course_id = x.split(" ")(1).toInt
      val score = x.split(" ")(2).toInt
      (stu_id, course_id, score)
    })
    score.filter(_._3>50).print()
    score.filter(_._3>50).print()

//    网络
    val NetLine = ssc.socketTextStream("localhost", 9999)
    val words = NetLine.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
