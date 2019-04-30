package com.tencent.angel.sona.ml

import java.util

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.variable.{PSVariable, VarState, VariableManager}
import com.tencent.angel.ml.math2.vector
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.sona.core.SparkEnvContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class SparkPSVariableManager private(isSparseFormat: Boolean) extends VariableManager {
  override def createALL[T](envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case SparkEnvContext(client: AngelPSClient) if client != null =>
        val matrixCtx = new util.ArrayList[MatrixContext]()
        getALLVariables.foreach {
          case variable: PSVariable =>
            val mCtx = variable.getMatrixCtx
            matrixCtx.add(mCtx)
            client.addMatrix(mCtx)
          case _ =>
        }
        client.createMatrices(matrixCtx)
        getALLVariables.foreach { variable => variable.setState(VarState.Created) }
      case _ =>
        getALLVariables.foreach { variable => variable.create(envCtx) }
    }
  }

  override def pullALL(epoch: Int, indices: vector.Vector): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    getALLVariables.foreach {
      case variable if isSparseFormat && variable.allowPullWithIndex =>
        variable.pull(epoch, indices)
      case variable => variable.pull(epoch)
    }
  }

  override def pull(name: String, epoch: Int, indices: vector.Vector): Unit = ???

  override def loadALL[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    envCtx match {
      case SparkEnvContext(client: AngelPSClient) if client != null =>
        val loadContext = new ModelLoadContext()
        getALLVariables.foreach { variable =>
          loadContext.addMatrix(new MatrixLoadContext(variable.name, path))
        }
        client.load(loadContext)
        getALLVariables.foreach { variable =>
          val varPath = new Path(path, variable.name).toString
          variable.load(SparkEnvContext(null), varPath, conf)
          variable.setState(VarState.Initialized)
        }
      case _ =>
        getALLVariables.foreach { variable => variable.load(envCtx, path, conf) }
    }
  }

  override def saveALL[T](envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case SparkEnvContext(client: AngelPSClient) if client != null =>
        val sharedConf = SharedConf.get()
        val saveContext = new ModelSaveContext
        getALLVariables.foreach { variable =>
          assert(variable.getState == VarState.Initialized || variable.getState == VarState.Ready)
          saveContext.addMatrix(new MatrixSaveContext(variable.name, variable.formatClassName))
        }
        //saveContext.setSavePath(sharedConf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH))
        saveContext.setSavePath(path)
        val deleteExistsFile = sharedConf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
          AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
        client.save(saveContext, deleteExistsFile)
      case _ =>
        getALLVariables.foreach { variable => variable.save(envCtx, path) }
    }
  }
}

object SparkPSVariableManager {
  private val sparkPSVariableManager = new ThreadLocal[SparkPSVariableManager]()

  def get(isSparseFormat: Boolean): SparkPSVariableManager = synchronized {
    if (sparkPSVariableManager.get() == null) {
      sparkPSVariableManager.set(new SparkPSVariableManager(isSparseFormat))
    }

    sparkPSVariableManager.get()
  }

  def getNew(isSparseFormat: Boolean): SparkPSVariableManager = {
    new SparkPSVariableManager(isSparseFormat)
  }
}
