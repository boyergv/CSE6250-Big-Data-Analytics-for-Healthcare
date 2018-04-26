/**

students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /** 
    Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients. 
    Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay. The given patientID should be excluded from the result.
    */

    /** Remove this placeholder and implement your code */

    val filteredgraph = graph.subgraph(vpred = (id,attr) => attr.isInstanceOf[PatientProperty]).collectNeighborIds(EdgeDirection.Out).map(x => x._1).collect().toSet
    val all_neighbors = graph.collectNeighborIds(EdgeDirection.Out)
    val filterNeighbor=all_neighbors.filter(f => filteredgraph.contains(f._1) && f._1.toLong != patientID)
    val setOfNeighbors=all_neighbors.filter(f=>f._1.toLong==patientID).map(f=>f._2).flatMap(f=>f).collect().toSet
    val patientScore=filterNeighbor.map(f=>(f._1,jaccard(setOfNeighbors,f._2.toSet)))
    patientScore.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2)).map(_._1.toLong).toList


   /*
   val neighbors = patients.collectNeighborIds(EdgeDirection.Out)
    val all_patients_as_neighbors = neighbors.filter{case(id,attr) => id != patientID}
    val this_patient_neighbors = neighbors.filter{case(id,attr) => id ==patientID}.map{case(id,attr)=> attr}.flatMap().collect().toSet
    val score = all_patients_as_neighbors.map{case(id,attr) => (id, jaccard(this_patient_neighbors, attr.toSet))}
    //val score = all_patients_as_neighbors.map(x => (x._1, jaccard(this_patient_neighbors, x._2.toSet)))
    score.takeOrdered(10)(Ordering[Double].reverse.on(x => x._2)).map(_._1.toLong).toList*/
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
    Given a patient, med, diag, lab graph, calculate pairwise similarity between all
    patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where 
    patient-1-id < patient-2-id to avoid duplications
    */

    /** Remove this placeholder and implement your code */
    val sc = graph.edges.sparkContext
    val filteredgraph = graph.subgraph(vpred = (id,attr) => attr.isInstanceOf[PatientProperty]).collectNeighborIds(EdgeDirection.Out).map(x => x._1).collect().toSet    
    val all_patients_neighbors = graph.collectNeighborIds(EdgeDirection.Out).filter(x => filteredgraph.contains(x._1))
    val cartesian_neighbors = all_patients_neighbors.cartesian(all_patients_neighbors).filter(x => x._1._1 < x._2._1)
    cartesian_neighbors.map(x => (x._1._1, x._2._1, jaccard(x._1._2.toSet, x._2._2.toSet)))

  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /** 
    Helper function

    Given two sets, compute its Jaccard similarity and return its result.
    If the union part is zero, then return 0.
    */
    
    /** Remove this placeholder and implement your code */
    if (a.isEmpty || b.isEmpty){return 0.0}
    a.intersect(b).size.toDouble/a.union(b).size.toDouble
  }
}
