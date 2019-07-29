package com.tencent.client.worker.ps.tensor

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.exception.{AngelException, InvalidParameterException}
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgent
import com.tencent.angel.psagent.matrix.MatrixClient
import com.tencent.client.common.Meta
import com.tencent.client.master.Master
import com.tencent.client.worker.ps.common.{EnvContext, MasterContext, State, WorkerContext}
import com.tencent.client.worker.ps.common.State.State
import com.tencent.client.worker.ps.variable.Initializer
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

abstract class TensorLike(val name: String,
                          val dim: Int,
                          val shape: Array[Long],
                          val dtype: String,
                          val validIndexNum: Long,
                          val initializer: Initializer) extends Logging {
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
          case ctx @ MasterContext(client: AngelPSClient, _) =>
            client.createMatrices(util.Arrays.asList(meta.getMatrixContext))
            while (matClient == null) {
              try {
                matClient = ctx.getPSAgent.getMatrixClient(meta.name)
              } catch {
                case e: InvalidParameterException =>
                  logInfo(e.getMessage)
                  Thread.sleep(50)
                  ctx.refreshMatrixInfo()
                case e: Exception =>
                  logInfo(e.getMessage)
                  Thread.sleep(50)
              }
            }
          case ctx @ WorkerContext(client: PSAgent, _) =>
            // cannot create matrix from worker

            // try to query matrix from ps, but it may not create yet
            // so we catch the exception and sleep (50 millis)
            // after that, we query again until we get the right data
            while (matClient == null) {
              try {
                matClient = client.getMatrixClient(meta.name)
              } catch {
                case e: InvalidParameterException =>
                  logInfo(e.getMessage)
                  Thread.sleep(50)
                  client.refreshMatrixInfo()
                case e: AngelException =>
                  logInfo(e.getMessage)
                  Thread.sleep(50)
                case e: Exception =>
                  logInfo(e.getMessage)
                  Thread.sleep(50)
              }
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

  def init[T](envCtx: EnvContext[T]): Unit = {
    writeLock.lock()

    try {
      if (state == State.Created) {
        if (meta.rowType.isDense && matClient != null) {
          envCtx match {
            case ctx @ MasterContext(_: AngelPSClient, _) =>
              matClient.update(initializer.getUpdateFunc(matClient.getMatrixId, meta)).get()
            case WorkerContext(_: PSAgent, _) =>
          }
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
          case ctx @ MasterContext(client: AngelPSClient, _) =>
            val modelLoadContext = new ModelLoadContext(path)
            modelLoadContext.addMatrix(meta.getMatrixLoadContext(path))
            client.load(modelLoadContext)
          case WorkerContext(_: PSAgent, _) =>
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
        case ctx @ MasterContext(client: AngelPSClient, _) =>
          val saveContext = new ModelSaveContext(path)
          saveContext.addMatrix(meta.getMatrixSaveContext(path, formatClassName))
          client.save(saveContext, true)
        case WorkerContext(_: PSAgent, _) =>
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
