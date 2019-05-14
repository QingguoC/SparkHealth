package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix


object NMF {

  /**
   * Run NMF clustering 
   * @param V The original non-negative matrix 
   * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
   * @param maxIterations The maximum number of iterations to perform
   * @param convergenceTol The maximum change in error at which convergence occurs.
   * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively 
   */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {

    /**
     * TODO 1: Implement your code here
     * Initialize W, H randomly
     * Calculate the initial error (Euclidean distance between V and W * H)
     */
    //val V1 = new RowMatrix(V.rows.zipWithIndex.repartition(1).repartition(10).sortBy(r=>r._2).map(r=>r._1))
    import org.apache.spark.mllib.random.RandomRDDs._
    val sc = V.rows.sparkContext
    V.rows.cache()
    val n = V.numRows().toInt
    val m = V.numCols().toInt
    val r = k


    val vforW = uniformVectorRDD(sc, n, r,seed = 8803L).map(v=>fromBreeze(toBreezeVector(v)))
    val initW = new RowMatrix(vforW)
    val boneW = new RowMatrix(V.rows.map(_ => BDV.rand[Double](r)).map(fromBreeze))
    var W = new RowMatrix(boneW.rows.zipWithIndex.map{ case (v, i) => i -> v }.join(initW.rows.zipWithIndex.map{ case (v, i) => i -> v }).sortByKey().map{case (i,(v1,v2)) => v2})
//var W = new RowMatrix(uniformVectorRDD(sc, n, r,seed = 8803L).repartition(1).repartition(10))
    val vforH = uniformVectorRDD(sc, r, m,seed = 8803L).map(v=>fromBreeze(toBreezeVector(v)))
    var H = getDenseMatrix(new RowMatrix(vforH))
    //var H = BDM.rand[Double](r,m)

    var err:Double = getError(V,W,H)
    var lastErr = 0.0
    var i = 0
    var errChange = abs((lastErr - err)/err)
    //var errChange = abs((lastErr - err))
    println(s"iteration i =  $i, error: $err")
    /**
     * TODO 2: Implement your code here
     * Iteratively update W, H in a parallel fashion until error falls below the tolerance value
     * The updating equations are,
     * H = H.* W^T^V ./ (W^T^W H)
     * W = W.* VH^T^ ./ (W H H^T^)
     */


    while(errChange > convergenceTol && i < maxIterations){


      W = dotProd(W,dotDiv(multiplyT(V,H.t),multiply(W,H * H.t)))
      W.rows.cache()
      H = H :* (computeWTV(W,V) :/ (computeWTV(W,W) * H ))


      err = getError(V,W,H)

      errChange = abs((lastErr - err)/err)
      //errChange = abs((lastErr - err))
      i = i + 1
      lastErr = err
      println(s"iteration i =  $i, error: $err")

    }

//
//    val boneW = new RowMatrix(V.rows.map(_ => BDV.rand[Double](r)).map(fromBreeze))
//    W = new RowMatrix(boneW.rows.zipWithIndex.map{ case (v, i) => i -> v }.join(W.rows.zipWithIndex.map{ case (v, i) => i -> v }).sortByKey().map{case (i,(v1,v2)) => v2})

    /** TODO: Remove the placeholder for return and replace with correct values */
    (W,H)
    //(new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache), BDM.rand[Double](k, V.numCols().toInt))
  }


  /**  
  * RECOMMENDED: Implement the helper functions if you needed
  * Below are recommended helper functions for matrix manipulation
  * For the implementation of the first three helper functions (with a null return), 
  * you can refer to dotProd and dotDiv whose implementation are provided
  */
  /**
  * Note:You can find some helper functions to convert vectors and matrices
  * from breeze library to mllib library and vice versa in package.scala
  */

    /** compute the mutiplication of a RowMatrix and a dense matrix */
    def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {
      //X.multiply(fromBreeze(d.t).transpose)
      X.multiply(fromBreeze(d))
    }
  def multiplyT(X: RowMatrix, d: BDM[Double]): RowMatrix = {
    X.multiply(fromBreeze(d.t).transpose)
    //X.multiply(fromBreeze(d))
  }
//  // refine W and H
//  def scaleDownRowMatrix(X: RowMatrix): RowMatrix = {
//    new RowMatrix(X.rows.map(r=>fromBreeze(toBreezeVector(r).map(a=>if(a>1e100) 1e100 else a))))
//  }

   /** get the dense matrix representation for a RowMatrix */
    def getDenseMatrix(X: RowMatrix): BDM[Double] = {
      val arr = X.rows.map(x => x.toArray).collect.transpose.flatten
      new BDM(X.numRows().toInt, X.numCols().toInt, arr)
    }

    /** matrix multiplication of W.t and V */
    def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {
      getDenseMatrix(W).t * getDenseMatrix(V)
    }


  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }


  def getError(V: RowMatrix, W: RowMatrix, H: BDM[Double]): Double ={
    val SumSquareErr = V.rows.zip(multiply(W,H).rows).map({case (v1,v2) => sum((toBreezeVector(v1) -toBreezeVector(v2)).map(a=>a*a))}).sum
    math.pow(SumSquareErr,0.5)
  }
}
