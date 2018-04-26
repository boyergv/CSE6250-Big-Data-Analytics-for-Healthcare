/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */

    

  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    val sc = patients.sparkContext
    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    val num_patient = patients.map(x => x.patientID).distinct().count()


    //diagnostic vertex
    val latest_diag = diagnostics.map(x => ((x.patientID, x.icd9code),x))
                        .reduceByKey((x1,x2) => if(x1.date>x2.date) x1 else x2)
                        .map{case(key,d) => d}
    val diag_index = latest_diag.map(_.icd9code).distinct().zipWithIndex().map{case(v,ind) => (v,ind+num_patient+1)}
    val diagVertexId = diag_index.collect.toMap
    val vertexDiagnostic: RDD[(VertexId, VertexProperty)] = diag_index
      .map{case(code,index)=> (index,DiagnosticProperty(code))}
    val num_diag = diag_index.count()


    //labresult vertex
    val latest_lab = labResults.map(x => ((x.patientID, x.labName),x))
                        .reduceByKey((x1,x2)=> if(x1.date>x2.date) x1 else x2)
                        .map{case(key,d) => d}
    val lab_index = latest_lab.map(_.labName).distinct().zipWithIndex().map{case(v,ind) => (v,ind+num_patient+1+num_diag)}
    val labVertexId = lab_index.collect.toMap
    val vertexLabresult: RDD[(VertexId, VertexProperty)] = lab_index
      .map{case(labname,index) => (index, LabResultProperty(labname))}
    val num_lab = lab_index.count()

    //medication vertex
    val latest_med = medications.map(x => ((x.patientID, x.medicine), x))
                        .reduceByKey((x1,x2) => if(x1.date>x2.date) x1 else x2)
                        .map{case(key,v) => v}
    val med_index = latest_med.map(_.medicine).distinct().zipWithIndex().map{case(v,ind) => (v,ind+num_patient+1+num_diag+num_lab)}
    val medVertexId = med_index.collect.toMap
    val vertexMedication: RDD[(VertexId, VertexProperty)] = med_index
      .map{case(medicine, index) => (index, MedicationProperty(medicine))}                    


    /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */
    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients
      .map({p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })

    //create the bi-directional edges for patients and diagnostics
    val bcdiagVertexId = sc.broadcast(diagVertexId)
    val edge_patient_diag = latest_diag
      .map(x => (x.patientID, x.icd9code, x))
      .map{case(patientId, icd9code, x) => 
      Edge(patientId.toLong, bcdiagVertexId.value(icd9code),PatientDiagnosticEdgeProperty(x).asInstanceOf[EdgeProperty])}
    val edge_diag_patient = latest_diag
      .map(x => (x.patientID, x.icd9code, x))
      .map{case(patientId, icd9code, x) => 
      Edge(bcdiagVertexId.value(icd9code),patientId.toLong, PatientDiagnosticEdgeProperty(x).asInstanceOf[EdgeProperty])}

    val patient_diag_edges = sc.union(edge_patient_diag, edge_diag_patient)


    //create the bi-directional edges for patients and labresults
    val bclabVertexId = sc.broadcast(labVertexId)
    val edge_patient_lab = latest_lab
      .map(x => (x.patientID, x.labName, x))
      .map{case(patientid, labName, x) =>
      Edge(patientid.toLong, bclabVertexId.value(labName), PatientLabEdgeProperty(x).asInstanceOf[EdgeProperty])}
    val edge_lab_patient = latest_lab
      .map(x => (x.patientID, x.labName, x))
      .map{case(patientid, labName, x) =>
      Edge(bclabVertexId.value(labName),patientid.toLong, PatientLabEdgeProperty(x).asInstanceOf[EdgeProperty])}

    val patient_lab_edges = sc.union(edge_patient_lab, edge_lab_patient)

    //create the bi-directional edges for patients and medications
    val bcmedVertexId = sc.broadcast(medVertexId)
    val edge_patient_med = latest_med
      .map(x => (x.patientID, x.medicine, x))
      .map{case(patientid, medicine, x) =>
      Edge(patientid.toLong, bcmedVertexId.value(medicine),PatientMedicationEdgeProperty(x).asInstanceOf[EdgeProperty])}
    val edge_med_patient = latest_med
      .map(x => (x.patientID, x.medicine, x))
      .map{case(patientid, medicine, x) =>
      Edge(bcmedVertexId.value(medicine),patientid.toLong,PatientMedicationEdgeProperty(x).asInstanceOf[EdgeProperty])}

    val patient_med_edges = sc.union(edge_patient_med, edge_med_patient)

    // Making Graph
    val vertices = sc.union(vertexPatient, vertexDiagnostic,vertexLabresult,vertexMedication)
    val edges = sc.union(patient_diag_edges, patient_lab_edges, patient_med_edges)
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)

    graph
  }
}
