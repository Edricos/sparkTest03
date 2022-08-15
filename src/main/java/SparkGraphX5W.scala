import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.Double.PositiveInfinity

object SparkGraphX5W {
  def main(args: Array[String]) {
    val fun1=1  //输出所有triplets
    val fun2=1  //输出年龄大于59的顶点
    val fun3=1  //输出属性大于1118的边
    val fun4=1  //输出边属性大于1100的triplets
    val fun5=1  //输出最大出度、入度、度
    val fun6=0  //所有顶点age+10
    val fun7=1  //所有边属性*2
    val fun8=0  //输出年龄大于59的子图
    val fun9=0  //连接操作：输出入度和出度相同的人，输出年龄最大的follower
    val fun10=1 //输出两点之间最短路径

    val conf = new SparkConf().setAppName("GraphX5W").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //设置顶点和边，注意顶点和边都是用元组定义的 Array
    //顶点的数据类型是 VD:(String,Int)
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型 ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    val begin = System.currentTimeMillis()
    var start = System.currentTimeMillis()

    //构造 vertexRDD 和 edgeRDD
    //val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
//    val linesVertexRDD:RDD[String]=sc.textFile("c:\\spark\\vertex5wn.txt")
//    val vertexRDD: RDD[(Long, (String, Int))]=linesVertexRDD.map(x=>{
//      val vertexId=x.split(" ")(0).toLong
//      val name=x.split(" ")(1)
//      val age=x.split(" ")(2).toInt
//      (vertexId,(name,age))
//    })
//    println("图中点数:"+vertexRDD.count())
//
//    //val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
//    val linesEdgeRDD:RDD[String]=sc.textFile("c:\\spark\\edge250w.txt")
//    val edgeRDD: RDD[Edge[Int]]=linesEdgeRDD.map(x=>{
//      val srcId=x.split(" ")(0).toInt
//      val dstId=x.split(" ")(1).toInt
//      val weight=x.split(" ")(2).toInt
//      Edge(srcId,dstId,weight)
//    })
//    println("图中边数:"+edgeRDD.count())

    //构造图 Graph[VD,ED]
    println("从文件中读取点和边数据，构造图Graph:")
    val graph: Graph[(String, Int), Int] = Graph(sc.parallelize(vertexArray), sc.parallelize(edgeArray))

    var end = System.currentTimeMillis()
    println("用时："+(end-start)+"毫秒")
    println("-------------------------------------------")

    if(fun1==1){
      println("打印所有的triplets：")
      graph.triplets.foreach(t => println(s"triplet:${t.srcId},${t.srcAttr},${t.dstId},${t.dstAttr},${t.attr}"))
    }

    if(fun2==1){
      start = System.currentTimeMillis()
      println("找出图中年龄大于 59 的顶点：")
      graph.vertices.filter { case (id, (name, age)) => age > 59 }.collect.foreach {
        //graph.vertices.filter { x => x._2._2 > 59 }.collect.foreach {
        case (id, (name, age)) => println(s"$name is $age")
      }
      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")
      println("-------------------------------------------")
    }

    if(fun3==1){
      start = System.currentTimeMillis()
      println("找出图中属性大于 1100 的边：")
      graph.edges.filter(e => e.attr > 1100).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")
      println("-------------------------------------------")
    }

    if(fun4==1){
      start = System.currentTimeMillis()
      println("列出边属性>1100 的 tripltes：")
      if(fun4==1)
        for (triplet <- graph.triplets.filter(t => t.attr > 1100).collect) {
          println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
        }
      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")
      println("-------------------------------------------")
    }

    if(fun5==1){
      start = System.currentTimeMillis()
      println("最大出度、入度、度：")
      def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
      }
      println("最大出度:" + graph.outDegrees.reduce(max) + ", 最大入度:" + graph.inDegrees.reduce(max) + ", 最大出度入度之和:" +
        graph.degrees.reduce(max))
      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")
      println("-------------------------------------------")
    }

    if(fun6==1){
      println("顶点的转换操作，顶点 age + 10：")
      graph.mapVertices { case (id, (name, age)) => (id, (name,
        age + 10))
      }.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    }

    if(fun7==1){
      start = System.currentTimeMillis()
      println("边的转换操作，边的属性*2：")
      //graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
      graph.mapEdges(e => e.attr * 2).edges.collect
      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")
      println("-------------------------------------------")
    }

    if(fun8==1){
      println("顶点年纪>59 的子图：")
      val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 59)
      println("子图所有顶点：")
      subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
      println
      println("子图所有边：")
      subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
      println
    }
    if(fun9==1){
      val inDegrees: VertexRDD[Int] = graph.inDegrees
      case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
      //创建一个新图，顶点 VD 的数据类型为 User，并从 graph 做类型转换
      val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age))
      => User(name, age, 0, 0)
      }
      //initialUserGraph 与 inDegrees、outDegrees（RDD）进行连接，并修改 initialUserGraph中 inDeg 值、outDeg 值
      val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
        case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
      }.outerJoinVertices(initialUserGraph.outDegrees) {
        case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
      }
      println("连接图的属性：")
      userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}"))
      println
      println("出度和入度相同的人员：")
      userGraph.vertices.filter {
        case (id, u) => u.inDeg == u.outDeg
      }.collect.foreach {
        case (id, property) => println(property.name)
      }
      println
      println("找出年纪最大的follower：")
      val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String,
        Int)](
        // 将源顶点的属性发送给目标顶点，map 过程
        et => et.sendToDst((et.srcAttr.name,et.srcAttr.age)),
        // 得到最大follower，reduce 过程
        (a, b) => if (a._2 > b._2) a else b
      )
      userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
        optOldestFollower match {
          case None => s"${user.name} does not have any followers."
          case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
        }
      }.collect.foreach { case (id, str) => println(str) }
      println
    }

    if(fun10==1){
      start = System.currentTimeMillis()
      println("最短路径：")
      print("请输入起点ID：")
      //val sourceId : VertexId = scala.io.StdIn.readInt()
      val sourceId : VertexId = 18888
      println("起点："+sourceId)
      // Initialize the graph such that all vertices except the root have distance infinity.
      val initialGraph: Graph[(Double, List[VertexId]), PartitionID] = graph.mapVertices((id, _) =>
        if (id == sourceId) (0.0, List[VertexId](sourceId))
        else (PositiveInfinity, List[VertexId]()))

      val sssp = initialGraph.pregel((PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(

        // Vertex Program
        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

        // Send Message
        triplet => {
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },
        //Merge Message
        (a, b) => if (a._1 < b._1) a else b)
      //println(sssp.vertices.collect.mkString("\n"))
      //println(sssp.vertices.filter{case(id,v) => id ==3})
      //var end = System.currentTimeMillis()
      //println("已建立起点"+sourceId+"到其它所有点的最短路径，用时："+(end-start)+"毫秒")

      print("请输入终点ID：")
      //val end_ID = scala.io.StdIn.readInt()
      val end_ID = 48888
      println("终点："+end_ID)

      println(sssp.vertices.collect.filter{case(id,v) => id == end_ID}.mkString("\n"))

      end = System.currentTimeMillis()
      println("用时："+(end-start)+"毫秒")


      println("总共用时："+(end-begin)+"毫秒")
    }

    sc.stop()
  }
}



