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
import partitioner.{Spinner, LabelPropagation, Maximum}
//import org.neo4j.spark.dataframe.Neo4jDataFrame

object MainGraphPartitioner {

//  implicit val partitionLoads = new LoadAccumulator();
//

  implicit val spark = SparkSession.builder()
    .appName("LearnScalaSpark")
    .config("spark.master", "local[*]")
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

    //val labelGraph = runOwnLabelPropagation(graph1,5)


    runSpinner( "data/Test/Synthetic_4.txt" )

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


  def runSpinner( path:String ): Unit ={

    println( "============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

    val graph = loadGraph(path)


    //println( graph.vertices )
    //println( graph.edges )


    val communities = Spinner.initialize( graph, 3, 5).vertices.collect()//.sortWith(_._1<_._1)
    //communities.foreach(println)
    //deleteNeo4J()
    //    saveNeo4J(graph)

    println( "\n=> Nodes (partitionId, nodes) " )
    sc.parallelize(communities).collect().foreach(println(_))
    //spark.sparkContext.parallelize(communities).map( n => (n._2, 1L) ).reduceByKey(_+_).sortByKey().take(20).foreach(println)

    println( "\n============================== "+Calendar.getInstance().getTime()+" ==============================\n" )

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
      .map(node => ( node._1, node._1) )

    val edgeList = spark.read.textFile(path).rdd
      .filter(l => !l.contains("#"))
      .map( l => Edge( l.split("\\s")(0).toInt, l.split("\\s")(1).toInt, 1L ) )

    val graph = Graph( nodeList, edgeList )

    graph.convertToCanonicalEdges( (l, r) => l+r )

  }
}
