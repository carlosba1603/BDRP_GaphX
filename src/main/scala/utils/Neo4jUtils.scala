package utils

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.neo4j.spark.Neo4j
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.neo4j.spark._
import org.graphframes._
import org.neo4j.spark.Neo4j.{NameProp, Pattern}

import scala.reflect.ClassTag

object Neo4jUtils {

  def deleteNeo4J()( implicit spark: SparkSession ): Unit ={




//    val rdd = Neo4j(spark.sparkContext).c("MATCH (d)-[r]-() delete r, d" ).loadRowRdd
//    //val rdd = Neo4j(spark.sparkContext).cypher("MATCH (d) detach delete d").loadRowRdd
//    println("Nodes in db: "+ rdd.count() )
  }

  def saveNeo4J[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED])( implicit spark: SparkSession ): Unit ={



    Neo4j(spark.sparkContext).saveGraph(graph, "partition", Pattern(NameProp("Node","vId"),scala.Seq(NameProp("Edge","weight")),NameProp("Node","vId")), merge = true)


//    val df = graph.edges.map( l => (l.srcId,l.dstId) ).toDF("src", "dst")
//
//    val renamedColumns = Map("src" -> "value", "dst" -> "value")
//
//    df.printSchema()
//    df.show()

//    Neo4jDataFrame.mergeEdgeList( spark.sparkContext, df, ("Node",Seq("src")),("w",Seq.empty),("Node",Seq("dst")), renamedColumns )

  }

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
