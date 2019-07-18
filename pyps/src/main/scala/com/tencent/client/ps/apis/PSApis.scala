package com.tencent.client.ps.apis


import com.tencent.client.apiserver.{FuncId, HandlerId}
import com.tencent.client.apiserver.protos.MSGProtos._
import com.tencent.client.apiserver.protos.MSGProtos.Request.DataCase
import com.tencent.client.apiserver.protos.ProtoUtils
import java.util

import com.google.protobuf.ByteString
import com.tencent.client.common.{DataHead, Deserializer, Executor, Utils}
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.psagent.PSAgentContext

import scala.collection.mutable
import com.tencent.client.ps.updater.Optimizer
import com.tencent.client.apiserver.plasma.PlasmaClient
import com.tencent.client.apiserver.protos.MSGProtos.Request
import com.tencent.client.apiserver.protos.MSGProtos.Response.Builder
import com.tencent.client.master.Master
import com.tencent.client.ps.tensor.Tensor
import com.tencent.client.ps.variable._
import com.tencent.client.worker.Worker


@HandlerId(1)
class PSApis(executor: Executor, client: PlasmaClient) {
  val timeoutMs: Int = 10000
  val sessions = new mutable.HashMap[Int, Session]()
  val cache = new mutable.HashMap[String, Matrix]()

  private def getInitializer(params: util.Map[String, String]): Initializer = {
    if (params != null && !params.isEmpty) {
      val mean = params.get("mean").toDouble
      val std = params.get("std").toDouble
      new NormalInitializer(mean, std)
    } else {
      new NormalInitializer(0.0, 1e-6)
    }
  }

  private def getUpdater(params: util.Map[String, String]): Updater = {
    if (params != null && !params.isEmpty) {
      val iter = params.entrySet.iterator()

      val scalaParams = (0 until params.size()).map { _ =>
        assert(iter.hasNext)
        val entey = iter.next()
        entey.getKey -> entey.getValue
      }.toMap

      Optimizer.get(scalaParams)
    } else {
      val default = Map("name" -> "Momentum", "lr" -> "0.0001")
      Optimizer.get(default)
    }
  }

  private def getErrorResponse(req: Request, respBuilder: Builder, message: String): Response = {
    respBuilder.setPid(req.getPid)
      .setFuncId(req.getFuncId)
      .setRet(0)
      .setErrorMsg(message)
      .build()
  }

  private def getErrorResponse(req: Request, respBuilder: Response.Builder, e: Exception): Response = {
    getErrorResponse(req, respBuilder, e.getMessage)
  }

  private def getSuccessResponse(req: Request, matId: Int, respBuilder: Response.Builder): Response.Builder = {
    respBuilder.setPid(req.getPid)
      .setFuncId(req.getFuncId)
      .setMatId(matId)
      .setRet(1)
  }

  private def toByteString(bytes: Array[Byte]): ByteString = {
    ByteString.copyFrom(bytes)
  }

  private def getSession(pid: Int): Session = synchronized{
    if (sessions.isEmpty) {
      val session = new Session(pid)
      executor match {
        case _: Master => session.setChief(true)
        case _: Worker => session.setChief(false)
      }

      sessions.put(pid, session)

      session
    } else if (sessions.contains(pid)) {
      sessions(pid)
    } else {
      val session = new Session(pid)
      session.setChief(false)
      sessions.put(pid, session)

      session
    }
  }

  @FuncId(1)
  def create(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    val session = getSession(req.getPid)

    req.getDataCase match {
      case DataCase.TENSOR =>
        try {
          val tMeta: TensorProto = req.getTensor
          val tensor = if (session.hasTensor(tMeta.getName)) {
            session.getTensor(tMeta.getName)
          } else {
            val initializer = getInitializer(tMeta.getInitializerParamsMap)
            val ts = new Tensor(tMeta.getName, tMeta.getDim, ProtoUtils.toArray(tMeta.getShapeList),
              tMeta.getDtype, tMeta.getValidIndexNum, initializer)

            executor match {
              case exe: Master =>
                if (session.isChief) {
                  ts.create(exe.getMasterContext)
                } else {
                  ts.create(exe.getWorkerContext)
                }
              case exe: Worker => ts.create(exe.context)
            }

            session.put(tMeta.getName, ts)
            ts
          }

          val matId = tensor.getMatClient.getMatrixId
          getSuccessResponse(req, matId, respBuilder).build()
        } catch {
          case e: Exception => getErrorResponse(req, respBuilder, e)
        }
      case DataCase.VARIABLE =>
        try {
          val vMeta = req.getVariable
          val variable = if (session.hasVariable(vMeta.getName)) {
            session.getVariable(vMeta.getName)
          } else {
            val updater = getUpdater(vMeta.getUpdaterParamsMap)
            val initializer = getInitializer(vMeta.getInitializerParamsMap)
            val va = new Variable(vMeta.getName, vMeta.getDim, ProtoUtils.toArray(vMeta.getShapeList),
              vMeta.getDtype, vMeta.getValidIndexNum, updater, initializer)

            executor match {
              case exe: Master =>
                if (session.isChief) {
                  va.create(exe.getMasterContext)
                } else {
                  va.create(exe.getWorkerContext)
                }
              case exe: Worker => va.create(exe.context)
            }

            session.put(vMeta.getName, va)

            va
          }

          val matId = variable.getMatClient.getMatrixId
          getSuccessResponse(req, matId, respBuilder).build()
        } catch {
          case e: Exception => getErrorResponse(req, respBuilder, e)
        }
      case _ => getErrorResponse(req, respBuilder, "invalidate params!")
    }
  }

  @FuncId(2)
  def init(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    try {
      val session = getSession(req.getPid)
      val tsLike = session.get(req.getMatId)

      executor match {
        case exe: Master =>
          if (session.isChief) {
            tsLike.init(exe.getMasterContext)
          } else {
            tsLike.init(exe.getWorkerContext)
          }
        case exe: Worker => tsLike.init(exe.context)
      }

      val matId = tsLike.getMatClient.getMatrixId
      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(3)
  def load(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    try {
      val session = getSession(req.getPid)
      val tsLike = session.get(req.getMatId)

      executor match {
        case exe: Master =>
          if (session.isChief) {
            tsLike.load(exe.getMasterContext, req.getLoadInfo.getPath, null)
          } else {
            tsLike.load(exe.getWorkerContext, req.getLoadInfo.getPath, null)
          }
        case exe: Worker => tsLike.load(exe.context, req.getLoadInfo.getPath, null)
      }

      val matId = tsLike.getMatClient.getMatrixId
      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(4)
  def pull(req: Request): Response = {
    val respBuilder = Response.newBuilder()

    try {
      val matId = req.getMatId
      val epoch = req.getEpoch
      val session = getSession(req.getPid)
      val tsLike = session.get(req.getMatId)
      val meta = tsLike.getMeta

      if (!req.hasObjectId && meta.rowType.isDense) {
        val result = tsLike.pull(epoch, null)
        val retObjId = PlasmaClient.getObjectId(req.getPid, matId, req.getEpoch, req.getBatch)
        client.put(retObjId, result, meta)
        getSuccessResponse(req, matId, respBuilder)
          .setObjectId(toByteString(retObjId))
          .build()
      } else if (req.hasObjectId) {
        val objectId = req.getObjectId.toByteArray
        val result = tsLike match {
          case em: Embedding =>
            val data = client.getMatrix(objectId, meta, timeoutMs)
            em.pull(epoch, data)
          case va: Variable =>
            val buf = client.getBuffer(objectId, timeoutMs)
            val dataHead = DataHead.fromBuffer(buf)
            val indices = Deserializer.indicesFromBuffer(buf, dataHead, meta)
            va.pull(epoch, Utils.vector2Matrix(indices))
        }

        val retObjId = PlasmaClient.getObjectId(req.getPid, matId, req.getEpoch, req.getBatch)
        client.put(retObjId, result, meta)
        getSuccessResponse(req, matId, respBuilder)
          .setObjectId(toByteString(retObjId))
          .build()
      } else {
        throw new Exception("there is no ObjectId for sparse pull!")
      }
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(5)
  def push(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    try {
      val matId = req.getMatId
      val session = getSession(req.getPid)
      val tsLike = session.get(req.getMatId)
      val meta = tsLike.getMeta

      assert(req.hasObjectId)
      val objectId = req.getObjectId.toByteArray
      val grad = client.getMatrix(objectId, meta, 10000)
      tsLike.push(grad, 0.0)


      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(6)
  def update(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    try {
      val session = getSession(req.getPid)
      val va = session.getVariable(req.getMatId)

      executor match {
        case exe: Master =>
          if (session.isChief) {
            va.update(exe.getMasterContext, req.getEpoch, req.getBatchSize)
          } else {
            va.update(exe.getWorkerContext, req.getEpoch, req.getBatchSize)
          }
        case exe: Worker => va.update(exe.context, req.getEpoch, req.getBatchSize)
      }

      val matId = req.getMatId
      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(7)
  def save(req: Request): Response = {
    val respBuilder = Response.newBuilder()
    try {
      val matId = req.getMatId
      val session = getSession(req.getPid)
      val va = session.getVariable(req.getMatId)
      val saveInfo = req.getSaveInfo

      executor match {
        case exe: Master =>
          if (session.isChief) {
            va.save(exe.getMasterContext, saveInfo.getPath, saveInfo.getFormatClassName)
          } else {
            va.save(exe.getWorkerContext, saveInfo.getPath, saveInfo.getFormatClassName)
          }
        case exe: Worker => va.save(exe.context, saveInfo.getPath, saveInfo.getFormatClassName)
      }

      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(8)
  def barrier(req: Request): Response = {
    val respBuilder = Response.newBuilder()

    try {
      PSAgentContext.get().barrier(req.getPid)
      getSuccessResponse(req, req.getMatId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(9)
  def clock(req: Request): Response = {
    val respBuilder = Response.newBuilder()

    try {
      val session = getSession(req.getPid)
      val tsLike = session.get(req.getMatId)

      tsLike.getMatClient.clock()

      getSuccessResponse(req, req.getMatId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }

  @FuncId(10)
  def release(req: Request): Response = {
    val respBuilder = Response.newBuilder()

    try {
      val session = getSession(req.getPid)

      executor match {
        case exe: Master =>
          if (session.isChief) {
            exe.getPSAgent.releaseMatrix(req.getMatId)
            session.remove(req.getMatId)
          } else {
            session.remove(req.getMatId)
          }
        case _: Worker => session.remove(req.getMatId)
      }



      getSuccessResponse(req, req.getMatId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }
}
