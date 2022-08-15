import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object secondSortKey {
  def main(args: Array[String]): Unit = {
    class SecondSort(val first:Int, val second:Int) extends Ordered[SecondSort] with Serializable {
      override def compare(that: SecondSort): Int = {
        if (this.first-that.first != 0){
          this.first-that.first
        }else {
          that.second - this.second
        }
      }
    }
    val conf = new SparkConf().setAppName("secondSortKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val txt:RDD[String] = sc.textFile("C:\\Users\\edric\\IdeaProjects\\sparkTest03\\src\\main\\java\\sort.txt", 1)
    val pair:RDD[(SecondSort, String)] = txt.map(line=>{
      ( new SecondSort(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line )
    })
    val pairSort : RDD[(SecondSort, String)] = pair.sortByKey()
    val result :RDD[String] = pairSort.map(_._2)
    result.foreach(println)
  }
}
