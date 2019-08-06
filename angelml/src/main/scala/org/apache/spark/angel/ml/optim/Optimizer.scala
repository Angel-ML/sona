package org.apache.spark.angel.ml.optim

import org.apache.spark.angel.ml.linalg
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.angel.ml.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for optimization problem solvers.
 */
@DeveloperApi
trait Optimizer extends Serializable {

  /**
   * Solve the provided convex optimization problem.
   */
  def optimize(data: RDD[(Double, linalg.Vector)], initialWeights: linalg.Vector): linalg.Vector
}
