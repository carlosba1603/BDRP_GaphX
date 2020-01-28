import java.io.File
import java.util.Calendar

import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.Queue
import partitioner.{LabelPropagation, Maximum, Spinner, VertexValue}
import utils.Neo4jUtils


object MainGraphPartitioner {

//  implicit val partitionLoads = new LoadAccumulator();
//

  implicit val spark = SparkSession.builder()
    .appName("LearnScalaSpark")
    .config("spark.master", "local[*]")
    .config("spark.neo4j.bolt.user", "neo4j")
    .config("spark.neo4j.bolt.password", "admin")
    .getOrCreate()

  val sc = spark.sparkContext

//  spark.sparkContext.register(partitionLoads, "Edge load by Partition")
  sc.setLogLevel("ERROR")

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  // Create a custom class to represent the GraphStatistics
  case class GraphStatistics(dbName: String, nodes: Long, edges: Long, maxDegree: Int, minDegree: Int, avgDegree: Double)
  case class GraphData(partition:Long)

  def main(args: Array[String]): Unit = {
    //val graphList = getGraphStatList("data")


    //runMax("data/Test/Synthetic_3.txt")
    //runLabelPropagation("data/liveJournal.txt")


    runSpinner( "data/Test/facebook.txt", 10, 20 )
    //Neo4jUtils.deleteNeo4J()
    //Neo4jUtils.saveNeo4J( graph )


    println("Done!")

//    print( partitionLoads.value.load )

  }




  def getGraphStat( path: String ): GraphStatistics ={

    val graph = loadGraph( path )

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


  def runSpinner( path:String, numberOfPartitions: Int, maxSteps: Int ): Unit ={

    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    val graph = loadGraph(path)

    val partitionedGraph = Spinner.initialize( graph, numberOfPartitions, maxSteps )
    val simpleGraph = partitionedGraph.mapVertices { case (vid,vInfo:VertexValue) => vInfo.currentPartition }

    println( "\n=> Nodes (nodeId, partitionId) " )
    simpleGraph.vertices.foreach(println(_))


    println( "\n=> Nodes (partitionId, nodes) " )
    simpleGraph.vertices.map( n => (n._2, 1L) ).reduceByKey(_+_).sortByKey().collect().foreach(println)

    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    Neo4jUtils.saveNeo4J(simpleGraph)

  }

  def runLabelPropagation( path:String ): Unit ={

    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    val graph = loadGraph(path)
    val communities = LabelPropagation.run( graph, 1).vertices.collect().sortWith(_._1<_._1)
    //communities.foreach(println)

    println( "\n=> Partitions (partitionId, nodes) " )
    spark.sparkContext.parallelize(communities).map( n => (n._2, 1L) ).reduceByKey(_+_).sortByKey().take(20).foreach(println)

    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

  }



  def runMax( path:String ): Unit ={


    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    val graph = loadGraph(path)
    val maxGraph = Maximum.run( graph, 10)
    maxGraph.vertices.collect().foreach(println)

    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

  }


  def loadGraph( path:String)( implicit spark: SparkSession): Graph[Long, Long] ={

    println("=> Processing "+ path+" ")

    val nodeList = spark.read.textFile(path).rdd
      .filter(l => !l.contains("#"))
      .flatMap( _.split("\\s") )
      .map( node => (node.toLong,1) )
      .reduceByKey(_+_)
      .sortBy( node => node )
      .map(node => (node._1, 0L) )

    val edgeList = spark.read.textFile(path).rdd
      .filter(l => !l.contains("#"))
      .map( l => Edge( l.split("\\s")(0).toInt, l.split("\\s")(1).toInt, 1L ) )

    val graph = Graph( nodeList, edgeList )

    graph.convertToCanonicalEdges( (l, r) => l+r )

  }
}
