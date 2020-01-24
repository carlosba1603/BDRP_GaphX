package spinner

case class LoadCounter(load: Map[String, Long] = Map() ) extends Serializable{

  // method to define logic for adding metric up during a transformation
  def add(partition: String): LoadCounter = {
    val existingCount = load.getOrElse(partition, 0L)

    this.copy(load = load.filterKeys{ key =>
                key != partition
              }
              ++
              Map(partition -> ( existingCount + 1L ) ))
  }

  // method to define logic for adding metric up during a transformation
  def add(partition: String, count: Long): LoadCounter = {
    val existingCount = load.getOrElse(partition, 0L)

    this.copy(load = load.filterKeys{ key =>
              key != partition
            }
              ++
              Map(partition -> ( existingCount + count ) ))
  }

  // method to define logic for merging two metric instances during an action
  def merge(other: LoadCounter): LoadCounter = {
    this.copy(load =  mergeMaps(this.load, other.load))
  }

  private def mergeMaps(l: Map[String, Long], r: Map[String, Long]): Map[String, Long] = {
    (l.keySet union r.keySet).map { key =>
      key -> (l.getOrElse(key, 0L) + r.getOrElse(key, 0L))
    }.toMap
  }
}
