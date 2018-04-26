/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object T2dmPhenotype {
  
  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  val DM_RELATED_DX = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648","648",
      "648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4")

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext
    val sqlContext = new SQLContext(sc)

    /** Hard code the criteria */
    val type1_dm_dx = Set("code1", "250.03")
    val type1_dm_med = Set("med1", "insulin nph")
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well

    //gather all the patient ids from the three RDDS
    val patients = diagnostic.map(_.patientID).union(labResult.map(_.patientID)).union(medication.map(_.patientID)).distinct()

    //Case patients
    val T1_Diag_yes = diagnostic.filter(x => T1DM_DX.contains(x.code)).map(_.patientID).distinct()
    val T1_Diag_no = patients.subtract(T1_Diag_yes).distinct()

    val T2_Diag_yes = diagnostic.filter(x => T2DM_DX.contains(x.code)).map(_.patientID).distinct()
    val T2_Diag_no = patients.subtract(T2_Diag_yes).distinct()

    val T1_Med_yes = medication.filter(x => T1DM_MED.contains(x.medicine.toLowerCase)).map(_.patientID).distinct()
    val T1_Med_no = patients.subtract(T1_Med_yes).distinct()

    val T2_Med_yes = medication.filter(x => T2DM_MED.contains(x.medicine.toLowerCase)).map(_.patientID).distinct()
    val T2_Med_no = patients.subtract(T2_Med_yes).distinct()

    val casePatient_one = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_no)
    val casePatient_two = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_yes).intersection(T2_Med_no)
    val casePatient_three_1 = T1_Diag_no.intersection(T2_Diag_yes).intersection(T1_Med_yes).intersection(T2_Med_yes)
    val case3_med = medication.map(x =>(x.patientID,x)).join(casePatient_three_1.map(x=>(x,0))).map(x => Medication(x._2._1.patientID,x._2._1.date,x._2._1.medicine))
    val case3_T1_med = case3_med.filter(x => T1DM_MED.contains(x.medicine.toLowerCase)).map(x => (x.patientID,x.date.getTime())).reduceByKey(Math.min)
    val case3_T2_med = case3_med.filter(x => T2DM_MED.contains(x.medicine.toLowerCase)).map(x => (x.patientID,x.date.getTime())).reduceByKey(Math.min)
    val casePatient_three = case3_T1_med.join(case3_T2_med).filter(x => x._2._1 > x._2._2).map(_._1)

    val case_Patients = sc.union(casePatient_one,casePatient_two,casePatient_three).distinct()
    
    
    //Control patients
    val first_glucose = labResult.filter(x => x.testName.toLowerCase.contains("glucose"))
    val glucose = first_glucose.map(_.patientID).distinct()
    val glucose_set = glucose.collect.toSet
    val patients_with_glucose = labResult.filter(x => glucose_set(x.patientID))
    val ab_lab_value = abnormal_lab_value(patients_with_glucose).distinct()
    val second = glucose.subtract(ab_lab_value).distinct()

    val third1 = diagnostic.filter(x => DM_RELATED_DX.contains(x.code)).map(x => x.patientID).distinct()
    val third2 = diagnostic.filter(x =>x.code.startsWith("250.")).map(x => x.patientID).distinct()
    val third = patients.subtract(third1.union(third2)).distinct()

    val control_Patients = second.intersection(third)

    //Other patients
    val other_Patients = patients.subtract(case_Patients).subtract(control_Patients).distinct()

    /** Find CASE Patients */
    val casePatients = case_Patients.map((_,1))

    /** Find CONTROL Patients */
    val controlPatients = control_Patients.map((_,2))

    /** Find OTHER Patients */
    val others = other_Patients.map((_,3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
  
  def abnormal_lab_value(labResult_g:RDD[LabResult]):RDD[String] = {
    val sc = labResult_g.sparkContext
    val ab1 = labResult_g.filter(x => x.testName.equals("hba1c") && x.value >= 6.0).map(x => x.patientID)
    val ab2 = labResult_g.filter(x=>x.testName.equals("hemoglobin a1c") && x.value >= 6.0).map(x =>x.patientID)
    val ab3 = labResult_g.filter(x => x.testName.equals("fasting glucose") && x.value >=110).map(x => x.patientID)
    val ab4 = labResult_g.filter(x => x.testName.equals("fasting blood glucose") && x.value>=110).map(x=>x.patientID)
    val ab5 = labResult_g.filter(x =>x.testName.equals("fasting plasma glucose") && x.value>=110).map(x =>x.patientID)
    //val ab6 = labResult_g.filter(x=> x.testName =="glucose" && x.value >110).map(x=>x.patientID)
    val ab7 = labResult_g.filter(x=> x.testName.equals("glucose") && x.value >110).map(x=>x.patientID)
    val ab8 = labResult_g.filter(x=>x.testName.equals("glucose, serum") && x.value >110).map(x =>x.patientID)

    val ab = sc.union(ab1,ab2,ab3,ab4,ab5,ab7,ab8)
    ab
  }


}
