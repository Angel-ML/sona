package org.apache.spark.angelml.optim

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.angelml.linalg.Vector
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
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector
}
