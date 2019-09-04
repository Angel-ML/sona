package org.apache.spark.linalg

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/**
  * Compute Cholesky decomposition.
  */
object CholeskyDecomposition {

  /**
    * Solves a symmetric positive definite linear system via Cholesky factorization.
    * The input arguments are modified in-place to store the factorization and the solution.
    *
    * @param A  the upper triangular part of A
    * @param bx right-hand side
    * @return the solution array
    */
  def solve(A: Array[Double], bx: Array[Double]): Array[Double] = {
    val k = bx.length
    val info = new intW(0)
    lapack.dppsv("U", k, 1, A, bx, k, info)
    checkReturnValue(info, "dppsv")
    bx
  }

  /**
    * Computes the inverse of a real symmetric positive definite matrix A
    * using the Cholesky factorization A = U**T*U.
    * The input arguments are modified in-place to store the inverse matrix.
    *
    * @param UAi the upper triangular factor U from the Cholesky factorization A = U**T*U
    * @param k   the dimension of A
    * @return the upper triangle of the (symmetric) inverse of A
    */
  def inverse(UAi: Array[Double], k: Int): Array[Double] = {
    val info = new intW(0)
    lapack.dpptri("U", k, UAi, info)
    checkReturnValue(info, "dpptri")
    UAi
  }

  private def checkReturnValue(info: intW, method: String): Unit = {
    info.`val` match {
      case code if code < 0 =>
        throw new IllegalStateException(s"LAPACK.$method returned $code; arg ${-code} is illegal")
      case code if code > 0 =>
        throw new Exception(
          s"LAPACK.$method returned $code because A is not positive definite. Is A derived from " +
            "a singular matrix (e.g. collinear column values)?")
      case _ => // do nothing
    }
  }

}
