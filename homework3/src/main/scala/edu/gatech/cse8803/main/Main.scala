/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat
import java.text.NumberFormat

import edu.gatech.cse8803.clustering.{NMF, Metrics}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
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
    val k_means = new KMeans().setSeed(8803L).setK(3).setMaxIterations(20).run(featureVectors).predict(featureVectors)
    val k_means_pred = features.map(_._1).zip(k_means).join(phenotypeLabel).map(_._2)
    //val k_means_cluster_one = k_means_pred.filter(x => (x._1 == 0 && x._2==1))
    val kMeansPurity = Metrics.purity(k_means_pred)


    /** TODO: GMMM Clustering using spark mllib
      *  Train a Gaussian Mixture model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 8803L
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/
    val gaussian = new GaussianMixture().setSeed(8803L).setK(3).setMaxIterations(20).run(featureVectors).predict(featureVectors)
    val gaussian_pred = features.map(_._1).zip(gaussian).join(phenotypeLabel).map(_._2)
    val gaussianMixturePurity = Metrics.purity(gaussian_pred)
  

    /** TODO: StreamingKMeans Clustering using spark mllib
      *  Train a StreamingKMeans model using the variabe featureVectors as input
      *  Set the number of cluster K = 3 and DecayFactor = 1.0, seed as 8803L and weight as 0.0
      *  please pay attention to the input type
      *  Assign each feature vector to a cluster(predicted Class)
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      */
    val num_dimensions = 10

    val streamingKmeans = new StreamingKMeans().setK(3).setRandomCenters(num_dimensions,0.0,8803L).latestModel()
    val streamingKmeans_pred = streamingKmeans.update(featureVectors,1.0,"batches").predict(featureVectors)
    val streamingKeans_test = features.map(_._1).zip(streamingKmeans_pred).join(phenotypeLabel).map(_._2)
    val streamKmeansPurity = Metrics.purity(streamingKeans_test)
   
    //val streamKmeansPurity = 0.0
    /** NMF */
    val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)
    // zip patientIDs with their corresponding cluster assignments
    // Note that map doesn't change the order of rows
    val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zip(assignments) 
    // join your cluster assignments and phenotypeLabel on the patientID and obtain a RDD[(Int,Int)]
    // which is a RDD of (clusterNumber, phenotypeLabel) pairs 
    val nmfClusterAssignmentAndLabel = assignmentsWithPatientIds.join(phenotypeLabel).map({case (patientID,value)=>value})
    // Obtain purity value
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)
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
    
    //diagnositc selection 
    val encounter_input = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_INPUT.csv", "diag")
    val encounter_dx_input = CSVUtils.loadCSVAsTable(sqlContext,"data/encounter_dx_INPUT.csv","diag_dx")

    val diagnostic_df  = sqlContext.sql("SELECT diag.Member_ID, diag.Encounter_DateTime, diag_dx.code FROM diag JOIN diag_dx ON diag.Encounter_ID = diag_dx.Encounter_ID")
    val diagnostic:RDD[Diagnostic] = diagnostic_df.map(x => Diagnostic(x(0).toString, dateFormat.parse(x(1).toString),x(2).toString))
    
    //labResult selection
    val lab_result = CSVUtils.loadCSVAsTable(sqlContext,"data/lab_results_INPUT.csv","lab")
    val lab_df_rdd = sqlContext.sql("SELECT Member_ID, Date_Resulted, Result_Name, Numeric_Result FROM lab").rdd
    val labResult:RDD[LabResult] = lab_df_rdd.filter(x =>(x.getString(3).nonEmpty) && (x.getString(3)!=null)).map(x=> LabResult(x(0).toString,dateFormat.parse(x(1).toString),x(2).toString.toLowerCase,(NumberFormat.getNumberInstance(java.util.Locale.US).parse(x(3).toString).doubleValue())))

    //medication selection
    val med_order = CSVUtils.loadCSVAsTable(sqlContext,"data/medication_orders_INPUT.csv","med")
    val med_df = sqlContext.sql("SELECT Member_ID, Order_Date, Drug_Name FROM med")
    val medication:RDD[Medication] = med_df.map(x => Medication(x(0).toString, dateFormat.parse(x(1).toString),x(2).toString.toLowerCase))

    
    //val medication: RDD[Medication] =  sqlContext.sparkContext.emptyRDD
    //val labResult: RDD[LabResult] =  sqlContext.sparkContext.emptyRDD
    //val diagnostic: RDD[Diagnostic] =  sqlContext.sparkContext.emptyRDD

    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
