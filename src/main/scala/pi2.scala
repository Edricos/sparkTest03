

import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi2 {
  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")
    val conf = new SparkConf()
      .setAppName("Spark Pi")
      .set("spark.executor.memory", "512m")
      //.set("spark.driver.host","192.168.171.1")//这个ip很重要，我因为这个ip没有设置正确卡了好长时间，我使用的是docker，这个ip就要设置为本机在docker分配的虚拟网卡中的ip地址，如果设置成其他网卡的ip会被主机拒绝访问
      .set("spark.driver.cores","2")
      .setMaster("spark://a1:7077") //这里应设为master的ip加上配置spark时设置的端口，一般都为7077，因为我使用的是docker所以将master的端口映射到本地了，所以直接用127.0.0.1来访问
      .setJars(List("C:\\unzip\\spark-3.0.3-bin-hadoop3.2\\spark-3.0.3-bin-hadoop3.2\\jars\\spark-core_2.12-3.0.3.jar"))

    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
