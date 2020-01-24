import java.io.File
import java.util.Calendar

import org.apache.spark.graphx.{Graph, VertexId, _}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.reflect.ClassTag
import scala.util.Random
import org.neo4j.spark._
import spinner.{LoadAccumulator, Spinner}
//import org.neo4j.spark.dataframe.Neo4jDataFrame

object MainGraphPartitioner {

  implicit val verticesDegree = new LoadAccumulator()
  implicit val spark = SparkSession.builder()
    .appName("LearnScalaSpark")
    .config("spark.master", "local[*]")
    .config("spark.neo4j.bolt.password", "admin")
    .getOrCreate()

  spark.sparkContext.register(verticesDegree, "Number of edges by vertex")
  spark.sparkContext.setLogLevel("ERROR")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  // Create a custom class to represent the GraphStatistics
  case class GraphStatistics(dbName: String, nodes: Long, edges: Long, maxDegree: Int, minDegree: Int, avgDegree: Double)
  case class GraphData(partition:Long)

  def main(args: Array[String]): Unit = {





    //val graphList = getGraphStatList("data")


    //runMax("data/Test/Synthetic_3.txt")
    //runLabelPropagation("data/liveJournal.txt")

    //val labelGraph = runOwnLabelPropagation(graph1,5)


    runSpinner( "data/Test/Synthetic_3.txt" )

    print( verticesDegree.value.load )

  }

  def deleteNeo4J(): Unit ={
    val rdd = Neo4j(spark.sparkContext).cypher("MATCH (d) detach delete d").loadRowRdd
    println("Nodes in db: "+ rdd.collect().length )
  }
//
//  def saveNeo4J[VD, ED: ClassTag](graph: Graph[VD, ED]): Unit ={
//    //Neo4j(spark.sparkContext).saveGraph(graph, "rank", Pattern(NameProp("Node"),scala.Seq(NameProp("Link")),NameProp("Node")), merge = true)
//
//    val df = graph.edges.map( l => (l.srcId,l.dstId) ).toDF("src", "dst")
//
//    val renamedColumns = Map("src" -> "value", "dst" -> "value")
//
//    df.printSchema()
//    df.show()
//
//    Neo4jDataFrame.mergeEdgeList( spark.sparkContext, df, ("Node",Seq("src")),("link",Seq.empty),("Node",Seq("dst")), renamedColumns )
//
//  }

//  def queryNEo4J(): Unit ={
//
//    rdd.collect().foreach(println)
//
//    println(rdd.first().schema.fieldNames)
//
//    val graphFrame = neo.pattern(("N","value"),("link",null), ("N","value")).loadGraphFrame
//
//    graphFrame.vertices.collect().foreach(println)
//  }




  def getGraphStat( path: String ): GraphStatistics ={

    val graph = Spinner.loadGraph( path )

    GraphStatistics( path.split("/")(1),
                     graph.numVertices,
                     graph.numEdges,
                     graph.degrees.map(_._2).max(),
                     graph.degrees.map(_._2).min(),
                     graph.degrees.map(_._2).sum()/graph.degrees.count()
                    )

  }

  def getGraphStatList( path: String ): Queue[GraphStatistics] ={
    val graphStatList : mutable.Queue[GraphStatistics] = Queue()

    val dir = new File(path)
    val files = dir.listFiles().filter(!_.isDirectory)

    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )
    files.foreach( f => graphStatList += getGraphStat(path+"/"+f.getName) )
    println()

    graphStatList.toDF().show()

    println( "============================== "+Calendar.getInstance().getTime()+"==============================\n" )

    graphStatList
  }


  def runSpinner( path:String ): Unit ={

    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    val graph = Spinner.loadGraph(path)


    //println( graph.vertices )
    //println( graph.edges )


    val communities = Spinner.initialize( graph, 1).vertices.collect()//.sortWith(_._1<_._1)
    //communities.foreach(println)
    //deleteNeo4J()
    //    saveNeo4J(graph)

    println( "\n=> Partitions (partitionId, nodes) " )
    //spark.sparkContext.parallelize(communities).map( n => (n._2, 1L) ).reduceByKey(_+_).sortByKey().take(20).foreach(println)

    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

  }

//  def runLabelPropagation( path:String ): Unit ={
//
//    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )
//
//    val graph = loadGraph(path)
//    val communities = labelPropagation( graph, 1).vertices.collect().sortWith(_._1<_._1)
//    //communities.foreach(println)
//
//    println( "\n=> Partitions (partitionId, nodes) " )
//    spark.sparkContext.parallelize(communities).map( n => (n._2, 1L) ).reduceByKey(_+_).sortByKey().take(20).foreach(println)
//
//    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )
//
//  }

  def labelPropagation[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }

    //lpaGraph.vertices.sortByKey().collect().foreach(println)

    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }

    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]): Map[VertexId, Long] = {
      // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
      val map = mutable.Map[VertexId, Long]()
      (count1.keySet ++ count2.keySet).foreach { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        map.put(i, count1Val + count2Val)
      }
      map.toMap
    }

    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) {
        attr
      }else {
        //println(vid+" = "+message+" result "+ message.maxBy(_._2)._1)
        message.maxBy(_._2)._1
      }
    }

    val initialMessage = Map[VertexId, Long]()

    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Both)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

//  def runMax( path:String ): Unit ={
//
//
//    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )
//
//    val graph = loadGraph(path)
//    val maxGraph = max( graph, 10)
//    maxGraph.vertices.collect().foreach(println)
//
//    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )
//
//  }

  def max[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val maxGraph = graph.mapVertices { case (vid, _) => vid }

    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Long )] = {

      if( e.srcAttr <= e.dstAttr ){
        Iterator()
      }else{
        Iterator( (e.dstId, e.srcAttr) )
      }

    }

    def mergeMessage( value1:Long, value2: Long ): Long = {
      Math.max(value1,value2)
    }

    def vertexProgram(vid: VertexId, attr: Long, message: Long ): VertexId = {
      if ( message == Long.MaxValue) { // superstep 0
        attr
      } else { // superstep > 0
        Math.max(attr, message)
      }
    }

    val initialMessage = Long.MaxValue

    Pregel(maxGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Either )(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

}
