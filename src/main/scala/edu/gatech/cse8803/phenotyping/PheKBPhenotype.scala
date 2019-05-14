/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

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

    /** Hard code the criteria */
    // val type1_dm_dx = Set("code1", "250.03")
    // val type1_dm_med = Set("med1", "insulin nph")
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well

    /** Find CASE Patients */

    val d1set = diagnostic.filter{case Diagnostic(_,_,code) => T1DM_DX.contains(code)}.map(d=>(d.patientID,1)).reduceByKey(_+_).map{case (d,c) => d}.collect.toSet

    val diagNot1 = diagnostic.filter{case Diagnostic(pid,_,_) => !(d1set.contains(pid))}

    val diag2 = diagNot1.filter{case Diagnostic(_,_,code) => T2DM_DX.contains(code)}.map(d=>(d.patientID,1)).reduceByKey(_+_).map{case (d,c) => d}
    val diag2set = diag2.collect.toSet
    val med1diag2set = medication.filter{case Medication(patientID, date, medicine) => (diag2set.contains(patientID) && T1DM_MED.exists{t1 => medicine.toLowerCase contains t1})}.map(m=>(m.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet
    val med1med2diag2 = medication.filter{case Medication(patientID, date, medicine) => (med1diag2set.contains(patientID) && T2DM_MED.exists{t2 => medicine.toLowerCase contains t2})}

    val med1med2diag2set = med1med2diag2.map(m=>(m.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val med1med2diag2EarliestMed2 = med1med2diag2.map{case Medication(patientID, date, medicine) =>(patientID,date)}.reduceByKey((d1,d2)=>if(d1.before(d2)) d1 else d2)

    val med1med2diag2EarliestMed1 = medication.filter{case Medication(patientID, date, medicine) => (med1med2diag2set.contains(patientID) && T1DM_MED.exists{t1 => medicine.toLowerCase contains t1})}.map{case Medication(patientID, date, medicine) =>(patientID,date)}.reduceByKey((d1,d2)=>if(d1.before(d2)) d1 else d2)

    val med1Precedemed1diag2set = med1med2diag2EarliestMed1.join(med1med2diag2EarliestMed2).filter{case (pid,(med1,med2)) => med1.before(med2)}.map{case (pid,(med1,med2)) => pid}.collect.toSet
    val casePatientsSet = diag2set.diff(med1Precedemed1diag2set)
    val casePatients = sc.parallelize(casePatientsSet.toSeq).map(pid => (pid,1))

    /** Find CONTROL Patients */
    val labGlu = labResult.filter(l => l.testName.toLowerCase contains "glucose")

    val gluPatientSet = labGlu.map(l=>(l.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val labGluP = labResult.filter(l => gluPatientSet.contains(l.patientID))

    val a1cSet = Set("hba1c","hemoglobin a1c")
    val gluSet = Set("fasting glucose","fasting blood glucose","fasting plasma glucose","glucose","glucose, serum")

    val pidAbnormLabSet = labGluP.filter(l => ((a1cSet.contains(l.testName.toLowerCase) && l.value >= 6.0) || (gluSet.contains(l.testName.toLowerCase) && l.value >= 110.0))).map(l => (l.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val labNormPset = labGluP.filter(l => !(pidAbnormLabSet.contains(l.patientID))).map(l=>(l.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val labNormDiabPset = diagnostic.filter(d => (labNormPset.contains(d.patientID) && (T1DM_DX.contains(d.code) || T2DM_DX.contains(d.code)))).map(d=>(d.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val controlPatientsSet = labNormPset.diff(labNormDiabPset)

    val controlPatients = sc.parallelize(controlPatientsSet.toSeq).map(pid => (pid,2))
    /** Find OTHER Patients */
    val allPatientSet = diagnostic.map(d => (d.patientID,1)).reduceByKey(_+_).map{case (p,c) => p}.collect.toSet

    val others = sc.parallelize(allPatientSet.diff(casePatientsSet).diff(controlPatientsSet).toSeq).map(pid => (pid,3))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
}
