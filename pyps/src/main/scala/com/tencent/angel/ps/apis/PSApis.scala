package com.tencent.angel.ps.apis

import com.tencent.angel.apiserver.plasma.PlasmaClient
import com.tencent.angel.apiserver.{FuncId, HandlerId}
import com.tencent.angel.ps.common.EnvContext
import com.tencent.angel.apiserver.protos.MSGProtos._
import com.tencent.angel.apiserver.protos.MSGProtos.Request.DataCase
import com.tencent.angel.apiserver.protos.ProtoUtils
import com.tencent.angel.ps.tensor.{Tensor, TensorLike}
import com.tencent.angel.ps.variable.{Embedding, Initializer, NormalInitializer, Updater, Variable}
import java.util

import com.google.protobuf.ByteString
import com.tencent.angel.common.{DataHead, Deserializer, Utils}
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.servingmath2.matrix._

import scala.collection.mutable
import com.tencent.angel.ps.updater.Optimizer


@HandlerId(1)
class PSApis[T](envCtx: EnvContext[T], client: PlasmaClient) {
  val tsMap = new mutable.HashMap[String, Tensor]()
  val varMap = new mutable.HashMap[String, Variable]()
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

  private def getErrorResponse(req: Request, respBuilder: Response.Builder, message: String): Response = {
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

  private def find(matId: Int): TensorLike = {
    val tensor = tsMap.values.collectFirst { case ts: Tensor if ts.getMatClient.getMatrixId == matId => ts }
    if (tensor.nonEmpty) {
      tensor.get
    } else {
      val variable = varMap.values.collectFirst { case va: Variable if va.getMatClient.getMatrixId == matId => va }
      if (variable.nonEmpty) {
        variable.get
      } else {
        throw new Exception("matrix no exist, please create first!")
      }
    }
  }

  @FuncId(1)
  def create(req: Request): Response = {
    val respBuilder = Response.newBuilder()

    req.getDataCase match {
      case DataCase.TENSOR =>
        try {
          val tMeta: TensorProto = req.getTensor
          val tensor = if (tsMap.contains(tMeta.getName)) {
            val initializer = getInitializer(tMeta.getInitializerParamsMap)
            val ts = new Tensor(tMeta.getName, tMeta.getDim, ProtoUtils.toArray(tMeta.getShapeList),
              tMeta.getDtype, tMeta.getValidIndexNum, initializer)
            tsMap.put(tMeta.getName, ts)
            ts.create(envCtx)

            ts
          } else {
            tsMap(tMeta.getName)
          }

          val matId = tensor.getMatClient.getMatrixId
          getSuccessResponse(req, matId, respBuilder).build()
        } catch {
          case e: Exception => getErrorResponse(req, respBuilder, e)
        }
      case DataCase.VARIABLE =>
        try {
          val vMeta = req.getVariable
          val variable = if (!varMap.contains(vMeta.getName)) {
            val updater = getUpdater(vMeta.getUpdaterParamsMap)
            val initializer = getInitializer(vMeta.getInitializerParamsMap)
            val va = new Variable(vMeta.getName, vMeta.getDim, ProtoUtils.toArray(vMeta.getShapeList),
              vMeta.getDtype, vMeta.getValidIndexNum, updater, initializer)
            va.create(envCtx)
            varMap.put(vMeta.getName, va)

            va
          } else {
            varMap(vMeta.getName)
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
      val tsLike = find(req.getMatId)
      tsLike.init()

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
      val matId = req.getMatId
      val tsLike = find(matId)

      tsLike.load(envCtx, req.getLoadInfo.getPath, null)

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
      val tsLike = find(matId)
      val meta = tsLike.getMeta

      if (!req.hasObjectId && meta.rowType.isDense) {
        val result = tsLike.pull(epoch, null)
        val retObjId = PlasmaClient.getObjectId(req.getPid, matId, req.getEpoch, req.getBatch)
        client.put(retObjId, result, meta)
        getSuccessResponse(req, matId, respBuilder)
          .setObjectId(toByteString(retObjId))
          .build()
      } else if (req.hasObjectId){
        val objectId = req.getObjectId.toByteArray
        val result = tsLike match {
          case em: Embedding =>
            val data = client.getMatrix(objectId, meta, 10000)
            em.pull(epoch, data)
          case va: Variable =>
            val buf = client.getBuffer(objectId, 10000)
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
      val tsLike = find(matId)
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
      val matId = req.getMatId
      find(matId) match {
        case va: Variable =>
          va.update(req.getEpoch, req.getBatchSize)
        case _ => throw new Exception("only variable can update!")
      }
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
      val tsLike = find(matId)
      val saveInfo = req.getSaveInfo
      tsLike.save(envCtx, saveInfo.getPath, saveInfo.getFormatClassName)
      getSuccessResponse(req, matId, respBuilder).build()
    } catch {
      case e: Exception => getErrorResponse(req, respBuilder, e)
    }
  }
}
