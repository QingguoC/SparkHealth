/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.clustering.{Metrics, NMF}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


import scala.io.Source


object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, streamKmeansPurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of StreamingKMeans is: $streamKmeansPurity%.5f")
    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

//    val numoffeatureTuples = featureTuples.count
//    val numofrawFeatures = rawFeatures.count
//    println(s"numoffeatureTuples, numofrawFeatures is:$numoffeatureTuples, $numofrawFeatures")
//    val numoffilteredFeatureTuples = filteredFeatureTuples.count
//    val numoffilteredRawFeatures = filteredRawFeatures.count
//    println(s"numoffilteredFeatureTuples, numoffilteredRawFeatures is:$numoffilteredFeatureTuples, $numoffilteredRawFeatures")

    val (kMeansPurity2, gaussianMixturePurity2, streamKmeansPurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of StreamingKMeans is: $streamKmeansPurity2%.5f")
    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")
    sc.stop 
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): (Double, Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})
    val rawFeatureVectors = features.map(_._2).cache()

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]
    /** transform a feature into its reduced dimension representation */
    def transform(feature: Vector): Vector = {
      Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
    }

    /** TODO: K Means Clustering using spark mllib
      *  Train a k means model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/
    featureVectors.cache()
    //val KMmodel = new KMeans().setK(3).setMaxIterations(20).setSeed(8803L).run(featureVectors)
    val KMmodel = KMeans.train(featureVectors,3,20,1,"random",8803L)
    val kmclusters = KMmodel.predict(featureVectors)
    val kmcluscls = features.map(_._1).zip(kmclusters).join(phenotypeLabel).map(_._2)
    val kMeansPurity = Metrics.purity(kmcluscls)
    println(s"K-means purity details")
    Metrics.printPurityDetail(kmcluscls)

    /** TODO: GMMM Clustering using spark mllib
      *  Train a Gaussian Mixture model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/

    val gmm = new GaussianMixture().setK(3).setSeed(8803L).setMaxIterations(20).run(featureVectors)
    val gmmclusters = gmm.predict(featureVectors)
    val gmmcluscls = features.map(_._1).zip(gmmclusters).join(phenotypeLabel).map(_._2)
    val gaussianMixturePurity = Metrics.purity(gmmcluscls)
    println(s"GMM purity details")
    Metrics.printPurityDetail(gmmcluscls)
    /** TODO: StreamingKMeans Clustering using spark mllib
      *  Train a StreamingKMeans model using the variabe featureVectors as input
      *  Set the number of cluster K = 3 and DecayFactor = 1.0, seed as 8803L and weight as 0.0
      *  please pay attention to the input type
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/


    var skm = new StreamingKMeans().setDecayFactor(1.0).setK(3).setRandomCenters(10,0,8803L).latestModel()
    skm.update(featureVectors,1.0,"batches")
    val skclusters = skm.predict(featureVectors)
    val skcluscls = features.map(_._1).zip(skclusters).join(phenotypeLabel).map(_._2)
    val streamKmeansPurity = Metrics.purity(skcluscls)
    println(s"Streaming K-means purity details")
    Metrics.printPurityDetail(skcluscls)


    /** NMF */
    //val rawFeaturesNonnegative = rawFeatureVectors.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    //val rawFeaturesNonnegative = featureVectors.map(f=>Vectors.dense(f.toArray.map(v=>Math.abs(v))))
    //val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)
    // zip patientIDs with their corresponding cluster assignments
    // Note that map doesn't change the order of rows
    val assignid = assignments.zipWithIndex().map(r=>(r._2,r._1))
    //val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zipWithIndex().map(r=>(r._2,r._1)).join(assignid).sortBy(r=>r._1).map{case (i,(p,c))=>(p,c)}
    val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zip(assignments)
    // join your cluster assignments and phenotypeLabel on the patientID and obtain a RDD[(Int,Int)]
    // which is a RDD of (clusterNumber, phenotypeLabel) pairs
    val nmfClusterAssignmentAndLabel = assignmentsWithPatientIds.join(phenotypeLabel).sortBy(r=>r._1).map({case (patientID,value)=>value})
    // Obtain purity value
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)
    println(s"NMF purity details")
    Metrics.printPurityDetail(nmfClusterAssignmentAndLabel)

    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity, nmfPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
    *
    * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /** You may need to use this date format. */
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    /** load data using Spark SQL into three RDDs and return them
      * Hint: You can utilize edu.gatech.cse8803.ioutils.CSVUtils and SQLContext.
      *
      * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
      *       Be careful when you deal with String and numbers in String type.
      *       Ignore lab results with missing (empty or NaN) values when these are read in.
      *       For dates, use Date_Resulted for labResults and Order_Date for medication.
      * */

    /** TODO: implement your own code here and remove existing placeholder code below */
    val encounter_INPUT = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_INPUT.csv", "encounter_INPUT")

    val encounter_dx_INPUT = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx_INPUT.csv", "encounter_dx_INPUT")

    val lab_results_INPUT = CSVUtils.loadCSVAsTable(sqlContext, "data/lab_results_INPUT.csv", "lab_results_INPUT")

    val medication_orders_INPUT = CSVUtils.loadCSVAsTable(sqlContext, "data/medication_orders_INPUT.csv", "medication_orders_INPUT")

    val diag_df = sqlContext.sql("SELECT encounter_INPUT.Member_ID patientID, encounter_INPUT.Encounter_DateTime date, encounter_dx_INPUT.code code FROM encounter_INPUT LEFT OUTER JOIN encounter_dx_INPUT ON encounter_INPUT.Encounter_ID = encounter_dx_INPUT.Encounter_ID")

    val diagnostic: RDD[Diagnostic] = diag_df.rdd.map(s => Diagnostic(s(0).asInstanceOf[String],dateFormat.parse(s(1).asInstanceOf[String]), s(2).asInstanceOf[String]))

    //val labResult: RDD[LabResult] = lab_results_INPUT.select("Member_ID","Date_Resulted","Test_Name","Numeric_Result").rdd.filter(s=>s(3) != "").map(s => LabResult(s(0).asInstanceOf[String],dateFormat.parse(s(1).asInstanceOf[String]),s(2).asInstanceOf[String],s(3).asInstanceOf[String].replaceAll(",","").toDouble))
    val labResult: RDD[LabResult] = lab_results_INPUT.select("Member_ID","Date_Resulted","Result_Name","Numeric_Result").rdd.filter(s=>s(3) != "").map(s => LabResult(s(0).asInstanceOf[String],dateFormat.parse(s(1).asInstanceOf[String]),s(2).asInstanceOf[String],s(3).asInstanceOf[String].replaceAll(",","").toDouble))

    val medication: RDD[Medication] = medication_orders_INPUT.select("Member_ID","Order_Date","Drug_Name").map(s => Medication(s(0).asInstanceOf[String],dateFormat.parse(s(1).asInstanceOf[String]),s(2).asInstanceOf[String]))

    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
