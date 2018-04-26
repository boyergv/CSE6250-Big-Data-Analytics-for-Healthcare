/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.util.Date

case class Diagnostic(patientID:String, date: Date, code: String)

case class LabResult(patientID: String, date: Date, testName: String, value: Double)

case class Medication(patientID: String, date: Date, medicine: String)
