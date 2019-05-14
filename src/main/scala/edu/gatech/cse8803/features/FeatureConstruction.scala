/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    diagnostic.map(d => ((d.patientID,d.code.toLowerCase),1.0)).reduceByKey(_+_)
    //diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    medication.map(m => ((m.patientID,m.medicine.toLowerCase),1.0)).reduceByKey(_+_)
    //medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val lsums = labResult.map(l => ((l.patientID,l.testName.toLowerCase),l.value)).reduceByKey(_+_)
    val lcounts = labResult.map(l => ((l.patientID,l.testName.toLowerCase),1.0)).reduceByKey(_+_)
    lsums.join(lcounts).map{case ((pid,lab),(sums,counts)) => ((pid,lab),sums/counts)}
    //labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    diagnostic.filter(d => candiateCode.contains(d.code.toLowerCase)).map(d => ((d.patientID,d.code.toLowerCase),1.0)).reduceByKey(_+_)
    //diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    medication.filter(m => candidateMedication.contains(m.medicine.toLowerCase)).map(m => ((m.patientID,m.medicine.toLowerCase),1.0)).reduceByKey(_+_)
    //medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val filtedLab = labResult.filter(l => candidateLab.contains(l.testName.toLowerCase))
    val lsums = filtedLab.map(l => ((l.patientID,l.testName.toLowerCase),l.value)).reduceByKey(_+_)
    val lcounts = filtedLab.map(l => ((l.patientID,l.testName.toLowerCase),1.0)).reduceByKey(_+_)
    lsums.join(lcounts).map{case ((pid,lab),(sums,counts)) => ((pid,lab),sums/counts)}
    //labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/

    /** transform input feature */

    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val featureMap = feature.map{case ((pid,f),v) => f}.distinct().sortBy(_.apply(0)).zipWithIndex()
    val featureSize = featureMap.count().toInt
    //sorted data by patientid an feature id
    val mappedFeature = feature.map{case ((pid,f),v) => (f,(pid,v))}.join(featureMap).map{case (f,((pid,v),featureID)) => (pid,featureID,v)}.sortBy(r=>(r._1,r._2))

    val result = mappedFeature.map(r=>(r._1,(r._2,r._3))).groupByKey().sortBy(r=>r._1).map{case (a,b) => (a,Vectors.sparse(featureSize, b.toArray.map(_._1.toInt),b.toArray.map(_._2)))}
    //println(s"featureSize is $featureSize")

    result
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */

  }
}


