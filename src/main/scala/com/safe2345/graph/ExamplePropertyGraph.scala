package com.safe2345.graph

import com.safe2345.utils.Session
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by zhangrb on 2017/7/7.
  */
class ExamplePropertyGraph extends Session {

  //  val userGraph: Graph[(String, String), String]

  // Assume the SparkContext has already been constructed

  // Create an RDD for the vertices
  val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
    (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
    Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

  // Define a default user in case there are relationship with missing user
  val defaultUser = ("John Doe", "Missing")
  // Build the initial Graph
  val graph = Graph(users, relationships, defaultUser)
  println(defaultUser)
  println(graph)
  relationships.foreach(x => println(x))
  graph.edges.foreach(x => println(x))
  graph.vertices.foreach(x => println(x))
  //  val graph: Graph[(String, String), String] // Constructed from above
  //  // Count all users which are postdocs
  graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }
    .foreach(x => println(x))
  //      .count
  // Count all the edges where src > dst
  graph.edges.filter(e => e.srcId > e.dstId)
  //      .foreach(x => println(x))
  //      .count
  graph.edges.filter { case Edge(src, dst, prop) => src > dst }
  //      .foreach(x => println(x))
  //      .count
  //    SELECT src.id, dst.id, src.attr, e.attr, dst.attr
  //    FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
  //    ON e.srcId = src.Id AND e.dstId = dst.Id
//  val graph = Graph(users, relationships, defaultUser)
  // Notice that there is a user 0 (for which we have no information) connected to users
  // 4 (peter) and 5 (franklin).
  graph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))
  // Remove missing vertices as well as the edges to connected to them
  val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
  // The valid subgraph will disconnect users 4 and 5 by removing user 0
  validGraph.vertices.collect.foreach(println(_))
  validGraph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))

}
