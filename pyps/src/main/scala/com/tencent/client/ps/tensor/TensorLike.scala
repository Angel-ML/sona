package com.tencent.client.ps.tensor

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.MatrixClient
import com.tencent.client.common.Meta
import com.tencent.client.ps.common.{EnvContext, MasterContext, WorkerContext, State}
import com.tencent.client.ps.common.State.State
import com.tencent.client.ps.variable.Initializer
import org.apache.hadoop.conf.Configuration

abstract class TensorLike(val name: String,
                          val dim: Int,
                          val shape: Array[Long],
                          val dtype: String,
                          val validIndexNum: Long,
                          val initializer: Initializer) {
  protected var state: State = State.New
  protected val lock = new ReentrantReadWriteLock()
  protected val readLock: ReentrantReadWriteLock.ReadLock = lock.readLock()
  protected val writeLock: ReentrantReadWriteLock.WriteLock = lock.writeLock()

  protected var matClient: MatrixClient = _
  protected val meta: Meta

  def getMeta: Meta = meta

  def getMatClient: MatrixClient = matClient

  def create[T](envCtx: EnvContext[T]): Unit = {
    writeLock.lock()

    try {
      if (state == State.New || state == State.Expired) {
        envCtx match {
          case MasterContext(client: AngelPSClient) =>
            client.createMatrices(util.Arrays.asList(meta.getMatrixContext))
          case WorkerContext(client: PSAgent) =>
            // cannot create matrix from worker
            if (matClient == null) {
              matClient = client.getMatrixClient(meta.name)
            }
        }

        // trans state
        if (state == State.New) {
          transSate(State.New, State.Created)
        } else {
          transSate(State.Expired, State.Created)
        }
      }
      assert(state == State.Created)
    } finally {
      writeLock.unlock()
    }
  }

  def init(): Unit = {
    writeLock.lock()

    try {
      if (state == State.Created) {
        if (meta.rowType.isDense && matClient != null) {
          matClient.update(initializer.getUpdateFunc(matClient.getMatrixId, meta)).get()
        }

        // trans stats
        transSate(State.Created, State.Initialized)
      }

      assert(state == State.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  def load[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    writeLock.lock()

    try {
      if (state == State.Created) {
        envCtx match {
          case MasterContext(client: AngelPSClient) =>
            val modelLoadContext = new ModelLoadContext(path)
            modelLoadContext.addMatrix(meta.getMatrixLoadContext(path))
            client.load(modelLoadContext)
          case WorkerContext(client: PSAgent) =>
            // cannot load matrix from worker
            if (matClient == null) {
              matClient = client.getMatrixClient(meta.name)
            }
        }

        // trans state
        transSate(State.Created, State.Initialized)
      }

      assert(state == State.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  def pull(epoch: Int, indices: Matrix): Matrix = {
    writeLock.lock()

    assert(matClient != null)
    try {
      assert(state != State.New && state != State.Expired)
      doPull(epoch, indices)

    } finally {
      writeLock.unlock()
    }
  }

  protected def doPull(epoch: Int, indices: Matrix): Matrix

  def push(grad: Matrix, alpha: Double): Unit = {
    writeLock.lock()

    assert(matClient != null)
    try {
      assert(state != State.New && state != State.Expired)
      doPush(grad, alpha)

    } finally {
      writeLock.unlock()
    }
  }

  protected def doPush(grad: Matrix, alpha: Double): Unit

  def save[T](envCtx: EnvContext[T], path: String, formatClassName: String): Unit = {
    readLock.lock()

    try {
      assert(state != State.New && state != State.Expired)

      envCtx match {
        case MasterContext(client: AngelPSClient) =>
          val saveContext = new ModelSaveContext(path)
          saveContext.addMatrix(meta.getMatrixSaveContext(path, formatClassName))
          client.save(saveContext, true)
        case WorkerContext(client: PSAgent) =>
          // cannot save matrix from worker
          if (matClient == null) {
            matClient = client.getMatrixClient(meta.name)
          }
      }

    } finally {
      readLock.unlock()
    }
  }

  protected def transSate(from: State, to: State): Unit = {
    assert(state == from)
    state = to
  }

  def setState(state: State): Unit = {
    this.state = state
  }

  def getState: State = state
}
