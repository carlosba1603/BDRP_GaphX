package partitioner

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

import org.apache.spark.graphx.VertexId

case class VertexValue( currentPartition:Int = -1, newPartition: Int = -1, degree: Long = 0L, active: Boolean = true ) extends Serializable {
  override def toString: String = s"Current: $currentPartition New: $newPartition Degree: $degree Active: $active"
}