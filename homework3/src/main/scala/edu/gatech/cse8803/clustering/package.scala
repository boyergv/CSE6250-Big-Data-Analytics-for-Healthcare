/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM, CSCMatrix => BSM, Matrix => BM}
import org.apache.spark.mllib.linalg.{Vectors, Matrices, Matrix, SparseMatrix, DenseMatrix,Vector, SparseVector, DenseVector}



package object clustering {
  def toBreezeVector(v: Vector): BV[scala.Double] = v match {
    case sp: SparseVector => new BSV[Double](sp.indices, sp.values, sp.size)
    case dp: DenseVector  => new BDV[Double](dp.values)
  }

  def toBreezeMatrix(m: Matrix): BM[scala.Double] = m match {
    case sm: SparseMatrix => new BSM[Double](sm.values, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices)
    case dm: DenseMatrix  => new BDM[Double](dm.numRows, dm.numCols, dm.values)
  }

  def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DenseVector(v.data)
        } else {
          new DenseVector(v.toArray)  // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(v.length, v.index, v.data)
        } else {
          new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.data)
      case sm: BSM[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
        new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }
}

