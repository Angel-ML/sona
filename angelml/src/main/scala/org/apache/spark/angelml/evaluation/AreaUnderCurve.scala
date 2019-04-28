package org.apache.spark.angelml.evaluation

import org.apache.spark.angelml.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD

/**
 * Computes the area under the curve (AUC) using the trapezoidal rule.
 */
private[evaluation] object AreaUnderCurve {

  /**
   * Uses the trapezoidal rule to compute the area under the line connecting the two input points.
   * @param points two 2D points stored in Seq
   */
  private def trapezoid(points: Seq[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve an RDD of ordered 2D points stored in pairs representing a curve
   */
  def of(curve: RDD[(Double, Double)]): Double = {
    curve.sliding(2).aggregate(0.0)(
      seqOp = (auc: Double, points: Array[(Double, Double)]) => auc + trapezoid(points),
      combOp = _ + _
    )
  }

  /**
   * Returns the area under the given curve.
   *
   * @param curve an iterator over ordered 2D points stored in pairs representing a curve
   */
  def of(curve: Iterable[(Double, Double)]): Double = {
    curve.toIterator.sliding(2).withPartial(false).aggregate(0.0)(
      seqop = (auc: Double, points: Seq[(Double, Double)]) => auc + trapezoid(points),
      combop = _ + _
    )
  }
}
