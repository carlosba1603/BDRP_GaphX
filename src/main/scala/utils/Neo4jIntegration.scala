package utils

import org.neo4j.spark.Neo4j

object Neo4jIntegration {
//  def deleteNeo4J()(): Unit ={
//    val rdd = Neo4j(sc).cypher("MATCH (d) detach delete d").loadRowRdd
//    println("Nodes in db: "+ rdd.collect().length )
//  }
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


}
