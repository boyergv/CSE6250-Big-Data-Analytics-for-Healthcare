package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /** 
    Given a patient ID, compute the random walk probability w.r.t. to all other patients. 
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */

    /** Remove this placeholder and implement your code */

    val personalized = patientID
    val src: VertexId = patientID

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    // When running personalized pagerank, only the source vertex
    // has an attribute 1.0. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
        if (!(id != src)) 1.0 else 0.0
      }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null

    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = {
        (src: VertexId, id: VertexId) => alpha * delta(src, id)
      } 

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - alpha) * msgSumOpt.getOrElse(0.0)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      //logInfo(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

      val filteredgraph = graph.subgraph(vpred = (id,attr) => attr.isInstanceOf[PatientProperty]).collectNeighborIds(EdgeDirection.Out).map(x => x._1).collect().toSet          
      val top_ten = rankGraph.vertices.filter(x => filteredgraph.contains(x._1)).takeOrdered(11)(Ordering[Double].reverse.on(x => x._2)).map(_._1)
      top_ten.slice(1,top_ten.length).toList

  }
}
