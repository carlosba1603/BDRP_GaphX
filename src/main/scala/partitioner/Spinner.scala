package partitioner

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object Spinner {


  def initialize[VD, ED: ClassTag](graph: Graph[VD, ED], numberOfPartitions: Int, maxSteps: Int)( implicit spark: SparkSession): Graph[VertexValue, ED] = {

    val partitionLoads = mutable.Map[Long,Long]()
    val candidatesLoads = mutable.Map[Long,Long]()
    val degreesMap = mutable.Map[Long,Long]();

    graph.ops.degrees.collect().foreach( d => degreesMap.put(d._1, d._2) )

    val totalEdges = degreesMap.map(l => l._2).reduce(_+_);
    val additionalCapacity = 0.3;
    val lambda = 1.0;
    val totalCapacity = Math.round( (1+additionalCapacity) * (totalEdges / numberOfPartitions) )


    println("Total Edges: "+totalEdges)
    println("Total Capacity: "+ totalCapacity)
    println("")


    val partitionLoadsBcst = spark.sparkContext.broadcast( partitionLoads )
    val candidatesLoadsBcst = spark.sparkContext.broadcast( candidatesLoads )

    val spinnerGraph = graph.mapVertices { case (vid, _) =>
      new VertexValue(
        degree = degreesMap.getOrElse(vid,0L)
    ) }

    def sendMessage(e: EdgeTriplet[VertexValue, ED]): Iterator[(VertexId, mutable.Map[Int, Long])] = {

      println(" Current: "+e)

      val w_u_v =  e.attr.asInstanceOf[Long]
      Iterator( ( e.dstId, mutable.Map(e.srcAttr.currentPartition -> w_u_v )), ( e.srcId, mutable.Map(e.dstAttr.currentPartition -> w_u_v ) ) )
    }

    def mergeMessage(count1: mutable.Map[Int, Long], count2: mutable.Map[Int, Long]): mutable.Map[Int, Long] = {
      // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
      val map = mutable.Map[Int, Long]()
      (count1.keySet ++ count2.keySet).foreach { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        map.put(i, count1Val + count2Val)
      }

      map
    }

    def vertexProgram(vId: VertexId, vInfo: VertexValue, message: mutable.Map[Int, Long])( implicit spark: SparkSession): VertexValue = {

      var currentPartition = vInfo.currentPartition
      var newPartition = vInfo.newPartition

      if (newPartition == -1) {

        var partition = Random.nextInt(numberOfPartitions)

        if( vId == 2 || vId == 5 ){
          partition = 0
        } else {
          partition = 1
        }

        var currentLoad = partitionLoadsBcst.value.getOrElse(partition,0L)
        partitionLoadsBcst.value.put(partition, currentLoad + vInfo.degree)

        currentPartition = partition
        newPartition = partition

      } else if ( currentPartition != newPartition ) {

        println("===== Decide Migration =====\n")

        val remainingCapacity = totalCapacity - partitionLoadsBcst.value.getOrElse( newPartition , 0L).toDouble
        val m_l = candidatesLoadsBcst.value.getOrElse( newPartition, 0L ).toDouble

        val migrationProb =  remainingCapacity / m_l
        val migrate = Random.nextDouble()

        println("VId: "+vId)
        println(s"r($newPartition) / m($newPartition): ( $remainingCapacity / $m_l ) = $migrationProb ")
        println("Degree: "+vInfo.degree)
        println("Current partition: "+ currentPartition )
        println("New partition: "+ newPartition )
        println(s"Migrate: $migrate p $migrationProb "+(migrate < migrationProb))


        if( migrationProb >= 1 || migrate < migrationProb  ) {

          println( partitionLoadsBcst.value )

          var currentLoad = partitionLoadsBcst.value.getOrElse(newPartition,0L)
          partitionLoadsBcst.value.put(newPartition, currentLoad + vInfo.degree)

          currentLoad = partitionLoadsBcst.value.getOrElse(currentPartition, 0L)
          partitionLoadsBcst.value.put(currentPartition, currentLoad - vInfo.degree)


          println( partitionLoadsBcst.value )
          currentPartition = newPartition

        }

        var currentLoad = candidatesLoadsBcst.value.getOrElse(newPartition,0L)
        candidatesLoadsBcst.value.put(newPartition, currentLoad - vInfo.degree)

        println("Candidates Load: " +candidatesLoadsBcst.value)

        println("")

        newPartition = currentPartition

      } else {


        println("===== Compute Scores =====\n")

        ( 0 to numberOfPartitions-1 ).foreach { i =>
          val current = message.getOrElse(i, 0L)
          message.put(i, current)
        }

        val denominator = message.map( e => e._2 ).reduce( _+_ ).toDouble

        val scoreMap = mutable.Map[Int, Double]()
        message.foreach( e => {
          val LPA = BigDecimal( e._2.toDouble/denominator ).setScale( 3, BigDecimal.RoundingMode.CEILING ).toDouble
          val PF =  BigDecimal( partitionLoadsBcst.value.getOrElse(e._1, 0L).toDouble / totalCapacity ).setScale( 3, BigDecimal.RoundingMode.CEILING ).toDouble
          scoreMap.put( e._1, lambda + LPA - PF )
        } )

        val candidates = scoreMap.filter( t => t._2 == scoreMap.map(l => l._2).max ).map(l => l._1 ).toIndexedSeq
        val candidatesCount = candidates.size

        if ( candidatesCount == 1 ) {
          newPartition = candidates(0)
        } else if( candidates.contains( currentPartition ) ) {
          newPartition = currentPartition
        } else {
          newPartition = candidates( Random.nextInt( candidatesCount ))
        }

        if( currentPartition != newPartition ){
          val currentLoad = candidatesLoadsBcst.value.getOrElse(newPartition,0L)
          candidatesLoadsBcst.value.put(newPartition, currentLoad + vInfo.degree)

        }

        println("Vid: "+vId)
        println("Partition Frequency: " + message)
        println("Partition Score: "+ scoreMap )
        println("Degree: "+vInfo.degree)
        println("Current partition: "+ currentPartition)
        println("New partition: "+ newPartition )

        println("")

      }

      VertexValue( currentPartition, newPartition, vInfo.degree )
    }


    val initialMessage: mutable.Map[Int, Long] = null

    Pregel(spinnerGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Both)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

}
