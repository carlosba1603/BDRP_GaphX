package spinner

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

import org.apache.spark.graphx.VertexId

class VertexValue( var currentPartition:Int = -1,  var newPartition: Int = -1, var degree: Long = 0L ) extends Serializable {

  override def equals(o: Any): Boolean = {
    if (this == o) return true
    if (o == null || (getClass != o.getClass)) return false
    val that = o.asInstanceOf[VertexValue]
    if (currentPartition != that.currentPartition || newPartition != that.newPartition) return false
    true
  }

  override def toString: String = s"Current: $currentPartition New: $newPartition Degree: $degree"
}