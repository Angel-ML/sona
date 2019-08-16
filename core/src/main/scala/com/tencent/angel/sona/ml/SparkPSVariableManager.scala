package com.tencent.angel.sona.ml

import java.util

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.mlcore.variable.{VarState, VariableManager}
import com.tencent.angel.ml.core.variable.PSVariable
import com.tencent.angel.ml.math2.vector
import com.tencent.angel.ml.matrix.psf.update.zero.Zero
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.{PSAgent, PSAgentContext}
import com.tencent.angel.sona.core.{SparkMasterContext, SparkWorkerContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class SparkPSVariableManager private(isSparseFormat: Boolean, sharedConf: SharedConf)
  extends VariableManager(isSparseFormat, sharedConf) {

  var isPSMatrixCreated: Boolean = false

  override def createALL[T](envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case SparkMasterContext(client: AngelPSClient) if client != null =>
        val matrixCtx = new util.ArrayList[MatrixContext]()
        getALLVariables.foreach {
          case variable: PSVariable =>
            if (!isPSMatrixCreated) {
              val mCtx = variable.getMatrixCtx
              matrixCtx.add(mCtx)
              client.addMatrix(mCtx)
            } else {
              val matrixId = PSAgentContext.get().getMatrixMetaManager.getMatrixId(variable.name)
              val zeroFunc = new Zero(new Zero.ZeroParam(matrixId, false))
              PSAgentContext.get().getPsAgent.getUserRequestAdapter.update(zeroFunc)
            }
          case _ =>
        }
        client.createMatrices(matrixCtx)
        getALLVariables.foreach { variable => variable.setState(VarState.Created) }
        isPSMatrixCreated = true
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
      case SparkMasterContext(client: AngelPSClient) if client != null =>
        val loadContext = new ModelLoadContext(path)
        getALLVariables.foreach { variable =>
          loadContext.addMatrix(new MatrixLoadContext(variable.name, path))
        }
        client.load(loadContext)
        getALLVariables.foreach { variable =>
          val varPath = new Path(path, variable.name).toString
          variable.load(SparkMasterContext(null), varPath, conf)
          variable.setState(VarState.Initialized)
        }
      case _ =>
        getALLVariables.foreach { variable => variable.load(envCtx, path, conf) }
    }
  }

  override def saveALL[T](envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case SparkMasterContext(client: AngelPSClient) if client != null =>
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

  override def releaseALL[T](envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case ctx @ SparkWorkerContext(client: PSAgent) if client != null =>
        getALLVariables.foreach {
          case variable: PSVariable => variable.release(ctx)
          case _ =>
        }

        variables.clear()
        slots.clear()
      case _ => throw new Exception("envCtx error!")
    }
  }
}

object SparkPSVariableManager {
  private val sparkPSVariableManager = new ThreadLocal[SparkPSVariableManager]()

  def get(isSparseFormat: Boolean, sharedConf: SharedConf): SparkPSVariableManager = synchronized {
    if (sparkPSVariableManager.get() == null) {
      sparkPSVariableManager.set(new SparkPSVariableManager(isSparseFormat, sharedConf))
    }

    sparkPSVariableManager.get()
  }

  def getNew(isSparseFormat: Boolean, sharedConf: SharedConf): SparkPSVariableManager = {
    new SparkPSVariableManager(isSparseFormat, sharedConf)
  }
}
