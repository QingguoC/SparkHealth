/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    clusterAssignmentAndLabel.map(r=>((r),1.0)).reduceByKey(_+_).map{case ((clust,cls),ct) => (clust,ct)}.groupByKey().map{case (a,b) => b.toArray.max}.collect.sum/clusterAssignmentAndLabel.count()
  }
  def printPurityDetail(clusterAssignmentAndLabel: RDD[(Int, Int)]) = {
    /**
      * TODO: Remove the placeholder and implement your code here
      */
    val clusclasscounts = clusterAssignmentAndLabel.map(r=>((r),1.0)).reduceByKey(_+_).map{case ((clust,cls),ct) => (cls,clust,ct)}.collect
    val caseCounts = clusclasscounts.filter(r=>r._1 == 1)
    val controlCounts = clusclasscounts.filter(r=>r._1 == 2)
    val unKnownCounts = clusclasscounts.filter(r=>r._1 == 3)

    val totalCaseCounts = caseCounts.map(r=>r._3).sum
    val totalControlCounts = controlCounts.map(r=>r._3).sum
    val totalUnknownCounts = unKnownCounts.map(r=>r._3).sum

    val clust1CountInCase = caseCounts.filter(r=>r._2 == 0)
    val clust2CountInCase = caseCounts.filter(r=>r._2 == 1)
    val clust3CountInCase = caseCounts.filter(r=>r._2 == 2)

    val clust1CountInControl = controlCounts.filter(r=>r._2 == 0)
    val clust2CountInControl = controlCounts.filter(r=>r._2 == 1)
    val clust3CountInControl = controlCounts.filter(r=>r._2 == 2)

    val clust1CountInUnknown = unKnownCounts.filter(r=>r._2 == 0)
    val clust2CountInUnknown = unKnownCounts.filter(r=>r._2 == 1)
    val clust3CountInUnknown = unKnownCounts.filter(r=>r._2 == 2)

    val clust1PercInCase = if(clust1CountInCase.length > 0) clust1CountInCase.map(r=>r._3).toList(0) * 100 /totalCaseCounts else 0
    val clust2PercInCase = if(clust2CountInCase.length > 0) clust2CountInCase.map(r=>r._3).toList(0) * 100 /totalCaseCounts else 0
    val clust3PercInCase = if(clust3CountInCase.length > 0) clust3CountInCase.map(r=>r._3).toList(0) * 100 /totalCaseCounts else 0

    val clust1PercInControl = if(clust1CountInControl.length > 0) clust1CountInControl.map(r=>r._3).toList(0) * 100 /totalControlCounts else 0
    val clust2PercInControl = if(clust2CountInControl.length > 0) clust2CountInControl.map(r=>r._3).toList(0) * 100 /totalControlCounts else 0
    val clust3PercInControl = if(clust3CountInControl.length > 0) clust3CountInControl.map(r=>r._3).toList(0) * 100 /totalControlCounts else 0

    val clust1PercInUnknown = if(clust1CountInUnknown.length > 0) clust1CountInUnknown.map(r=>r._3).toList(0) * 100 /totalUnknownCounts else 0
    val clust2PercInUnknown = if(clust2CountInUnknown.length > 0) clust2CountInUnknown.map(r=>r._3).toList(0) * 100 /totalUnknownCounts else 0
    val clust3PercInUnknown = if(clust3CountInUnknown.length > 0) clust3CountInUnknown.map(r=>r._3).toList(0) * 100 /totalUnknownCounts else 0

    println(s"Case cluseter 1: $clust1PercInCase")
    println(s"Case cluseter 2: $clust2PercInCase")
    println(s"Case cluseter 3: $clust3PercInCase")

    println(s"Control cluseter 1: $clust1PercInControl")
    println(s"Control cluseter 2: $clust2PercInControl")
    println(s"Control cluseter 3: $clust3PercInControl")

    println(s"Unknown cluseter 1: $clust1PercInUnknown")
    println(s"Unknown cluseter 2: $clust2PercInUnknown")
    println(s"Unknown cluseter 3: $clust3PercInUnknown")
  }
}
