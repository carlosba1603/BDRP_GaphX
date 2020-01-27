package partitioner

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}

import scala.reflect.ClassTag

object Maximum {
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
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
