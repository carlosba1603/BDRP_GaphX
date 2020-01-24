package spinner

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object Spinner {

  private val DEFAULT_NUM_PARTITIONS = 8;
  val numberOfPartitions = 4;

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

  def initialize[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int)( implicit spark: SparkSession): Graph[VertexValue, ED] = {

    val degrees = mutable.Map[Long,Long]();
    graph.ops.degrees.collect().foreach( d => degrees.put(d._1, d._2) )

    val spinnerGraph = graph.mapVertices { case (vid, _) => println(vid)
      new VertexValue(
        degree = degrees.getOrElse(vid,0L)
    ) }

    val rand = new Random()

    def sendMessage(e: EdgeTriplet[VertexValue, ED]): Iterator[(VertexId, Map[Int, Long])] = {

      //verticesDegree.add(e.srcId.toLong);

      println(" Current: "+e)
      //val currentScore = graph.edges.filter( ex => ex.dstId == e.dstId || ex.srcId == e.dstId ) )

      //println("Message: (" + e.srcId + ", Map(" + e.dstAttr.currentPartition + " -> 1L) , (" + e.dstId + ", Map(" + e.srcAttr.currentPartition + " -> 1L)")
      Iterator((e.srcId, Map(e.dstAttr.currentPartition -> 1L)))//, (e.dstId, Map(e.srcAttr.currentPartition -> 1L))
    }

    def mergeMessage(count1: Map[Int, Long], count2: Map[Int, Long]): Map[Int, Long] = {
      // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
      val map = mutable.Map[Int, Long]()
      (count1.keySet ++ count2.keySet).foreach { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        map.put(i, count1Val + count2Val)
      }
      map.toMap
    }

    def vertexProgram(vId: VertexId, vInfo: VertexValue, message: Map[Int, Long]): VertexValue = {

      //assign the previous label
      var currentPartition = vInfo.currentPartition
      var newPartition = vInfo.newPartition

      //(ii) If the vertex is new, assign a random partition
      if (currentPartition == -1) {
        val partition = rand.nextInt(numberOfPartitions)


        //graph.edges.collect().foreach(println(_))
//        partitionLoads.add( partition,  vInfo. )
//
//        println("ACcumulator"+partitionLoads.value)

        currentPartition = partition
        newPartition = partition

      } else {

//        println(" V: " + vId + ", M: " + message)


        //graph.edges.collect().foreach(println(_))
        //computeNewPartition( message )



        //newPartition = computeNewPartition()

      }

      vInfo.currentPartition = currentPartition
      vInfo.newPartition = newPartition
      vInfo
    }


    val initialMessage: Map[Int, Long] = null

    Pregel(spinnerGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Both)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

//  def computeNewPartition( partitionFrequency: Map[Int, Long])( implicit partitionLoads: LoadAccumulator): Unit = {
//
//    val bestState = Double.MinValue
//    val currentState = 0
//
//    var possiblePartitions = mutable.ArraySeq[Int]()
//
//    val totalLabels = partitionFrequency.map(l => l._2).reduce(_ + _)
//    val LPA = partitionFrequency.map(l => (l._1, l._2.toDouble / totalLabels))
//    //val W = partitionLoads.value.loads.foreach( p => (p._1, p._2/ ))
//
//
//
//    //LPA.foreach(println(_))
//
//  }
}
