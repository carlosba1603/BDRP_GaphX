package spinner

import org.apache.spark.util.AccumulatorV2

case class LoadAccumulator(var acc: LoadCounter = LoadCounter() ) extends AccumulatorV2[ String, LoadCounter] {
  override def isZero: Boolean = {
    acc.load.isEmpty
  }

  override def copy(): AccumulatorV2[String, LoadCounter] = {
    new LoadAccumulator( acc )
  }

  override def reset(): Unit = {
    acc = new LoadCounter()
  }

  override def add(input: String): Unit = {
    acc = acc.add(input)
  }

  def add(input: String, count: Long): Unit = {
    acc = acc.add(input, count)
  }

  override def merge(other: AccumulatorV2[String, LoadCounter]): Unit = {
    acc = acc.merge( other.value )
  }

  override def value: LoadCounter = {
    acc
  }
}
