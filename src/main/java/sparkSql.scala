
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, upper}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession.implicits._

object sparkSql {
  private val master = "spark://a1:7077"
  private val remote_file = "hdfs://a1:9000/test/wc.txt"

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")
    val conf = new SparkConf()
      .setAppName("sparkSql")
      .setMaster("local")

//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().appName("sparkSql").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    var jdbc = spark.read.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://rm-uf6r00howqec9yh2m8o.mysql.rds.aliyuncs.com:3306/test?serverTimezone=GMT%2B8",
        "user" -> "edric",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "password" -> "TcGJnl1r594TuC2h7C",
        "query" -> "select * from account"
      )).load()
    //      jdbc.show()

    import spark.implicits._
    jdbc.select(upper($"username").as("wcname")).show()

    jdbc.createTempView("acct")
    val rst = spark.sql("select * from acct where id = 1")
    rst.show()

    jdbc.select("id").show()

    jdbc.filter($"id" > 5).show()

    jdbc.groupBy("password").count().show()




  }
}
