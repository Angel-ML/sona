package com.tencent.angel.ps.tensor

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.apiserver.FuncId
import com.tencent.angel.client.AngelClient
import com.tencent.angel.common.Meta
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.vector.Vector
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.ps.common.State.State
import com.tencent.angel.ps.common.{EnvContext, MasterContext, State, WorkerContext}
import com.tencent.angel.ps.variable.Initializer
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.MatrixClient
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

  @transient protected var matClient: MatrixClient = _
  @transient protected val meta: Meta

  def getMeta: Meta = meta

  def getMatClient: MatrixClient = matClient

  @FuncId(1)
  def create[T](envCtx: EnvContext[T]): Unit = {
    writeLock.lock()

    try {
      if (state == State.New || state == State.Expired) {
        envCtx match {
          case MasterContext(client: AngelClient) =>
            client.createMatrices(util.Arrays.asList(meta.getMatrixContext))
          case WorkerContext(client: PSAgent) =>
            // cannot create matrix from worker
            if (matClient == null) {
              matClient = client.getMatrixClient(meta.getName)
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

  @FuncId(2)
  def init(taskFlag: Int): Unit = {
    writeLock.lock()

    try {
      if (state == State.Created) {
        if (meta.getRowType.isDense && matClient != null) {
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

  @FuncId(3)
  def load[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    writeLock.lock()

    try {
      if (state == State.Created) {
        envCtx match {
          case MasterContext(client: AngelClient) =>
            val modelLoadContext = new ModelLoadContext(path)
            modelLoadContext.addMatrix(meta.getMatrixLoadContext(path))
            client.load(modelLoadContext)
          case WorkerContext(client: PSAgent) =>
            // cannot load matrix from worker
            if (matClient == null) {
              matClient = client.getMatrixClient(meta.getName)
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

  @FuncId(4)
  def pull(epoch: Int, indices: Vector): Array[ByteBuffer] = {
    writeLock.lock()

    assert(matClient != null)
    var pulled: Array[ByteBuffer] = null
    try {
      if (state != State.New && state != State.Expired) {
        val headBuf = ByteBuffer.allocate(128)
        val params = doPull(epoch, indices)
        pulled = Array(headBuf, SerDeUtils.vecArray2Byte(params))

        // trans state
        if (state == State.Initialized) {
          transSate(State.Initialized, State.Ready)
        } else if (state == State.Created) {
          transSate(State.Created, State.Ready)
        }
      }

      assert(state == State.Ready)

      pulled
    } finally {
      writeLock.unlock()
    }
  }

  protected def doPull(epoch: Int, indices: Vector): Array[Vector]

  @FuncId(5)
  def push(grad: Array[Byte], alpha: Double): Unit = {
    writeLock.lock()

    assert(matClient != null)
    try {
      assert(state != State.New && state != State.Expired)
      doPush(SerDeUtils.byte2Matrix(grad), alpha)
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
        case MasterContext(client: AngelClient) =>
          val saveContext = new ModelSaveContext(path)
          saveContext.addMatrix(meta.getMatrixSaveContext(path, formatClassName))
          client.save(saveContext, true)
        case WorkerContext(client: PSAgent) =>
          // cannot save matrix from worker
          if (matClient == null) {
            matClient = client.getMatrixClient(meta.getName)
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
