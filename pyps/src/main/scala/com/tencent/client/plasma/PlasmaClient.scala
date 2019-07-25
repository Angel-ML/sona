package com.tencent.client.plasma

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, ThreadLocalRandom, TimeUnit}
import java.util.logging.Logger
import java.util.Random

import com.tencent.client.plasma.exceptions.{DuplicateObjectException, PlasmaOutOfMemoryException}
import com.tencent.client.common.{DataHead, Deserializer, Meta, Serializer}
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.LabeledData
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ShutdownHookManager

import scala.collection.JavaConversions._


class PlasmaClient private(storeSocketName: String, managerSocketName: String, releaseDelay: Int) {
  private lazy val conn = PlasmaClientJNI.connect(storeSocketName, managerSocketName, releaseDelay)

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), null)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer, metadata: Array[Byte]): Unit = {
    val tmpBuf = PlasmaClientJNI.create(conn, objectId, buf.remaining(), metadata)
    tmpBuf.put(buf)
    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], buf: ByteBuffer, metadata: ByteBuffer): Unit = {
    val meta = new Array[Byte](metadata.remaining())
    metadata.get(meta)
    put(objectId, buf, metadata)
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], mat: Matrix, meta: Meta): Array[Byte] = {
    val dataHead: DataHead = Serializer.getDataHeadFromMatrix(mat, meta)
    val buf = PlasmaClientJNI.create(conn, objectId, DataHead.headLen + dataHead.length, null)

    Serializer.putMatrix(buf, mat, meta, dataHead)

    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)

    objectId
  }

  @throws[DuplicateObjectException]
  @throws[PlasmaOutOfMemoryException]
  def put(objectId: Array[Byte], vec: Vector, meta: Meta): Array[Byte] = {
    val dataHead: DataHead = Serializer.getDataHeadFromVector(vec, meta)
    val buf = PlasmaClientJNI.create(conn, objectId, DataHead.headLen + dataHead.length, null)

    Serializer.putVector(buf, vec, meta, dataHead)

    PlasmaClientJNI.seal(conn, objectId)
    PlasmaClientJNI.release(conn, objectId)

    objectId
  }

  def getMatrix(objectId: Array[Byte], meta: Meta, timeoutMs: Int): Matrix = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    val dataBuf = bufs(0)(0)
    val metaBuf = bufs(0)(1)

    Deserializer.matrixFromBuffer(dataBuf, meta)
  }

  def getVector(objectId: Array[Byte], meta: Meta, timeoutMs: Int): Vector = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    val dataBuf = bufs(0)(0)
    val metaBuf = bufs(0)(1)

    Deserializer.vectorFromBuffer(dataBuf, meta)
  }

  def getBuffer(objectId: Array[Byte], timeoutMs: Int): ByteBuffer = {
    val bufs = PlasmaClientJNI.get(conn, Array[Array[Byte]](objectId), timeoutMs)
    assert(bufs.length == 1)

    bufs(0)(0)
  }

  def putBatch(data: Array[LabeledData]): Array[Byte] = {
    null
  }

  def seal(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.seal(conn, objectId)
  }

  def release(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.release(conn, objectId)
  }

  def delete(objectId: Array[Byte]): Unit = {
    PlasmaClientJNI.delete(conn, objectId)
  }

  def contains(objectId: Array[Byte]): Boolean = {
    PlasmaClientJNI.contains(conn, objectId)
  }
}

object PlasmaClient {
  private val logger = Logger.getLogger(PlasmaClient.getClass.getSimpleName)

  val Plasma_Store_Path = "plasma.store.path"
  val Plasma_Store_Suffix = "plasma.store.suffix"
  val Plasma_Store_MemoryGB = "plasma.store.memoryGB"

  private val rand = new Random()
  private var storeProcess: Process = _
  private var plasmaName: String = _
  private val clientPool = new ConcurrentLinkedQueue[PlasmaClient]()

  private val shutdownHookManager = ShutdownHookManager.get()
  private var stopPlasmaHookTask: Runnable = _

  def getObjectId(matId: Int, epoch: Int, batch: Int): Array[Byte] = {
    val bytes = new Array[Byte](20)
    val buf = ByteBuffer.wrap(bytes)

    buf.putInt(matId)
    buf.putInt(epoch)
    buf.putInt(batch)

    bytes
  }

  def randomObjectId: Array[Byte] = {
    val bytes = new Array[Byte](20)
    rand.nextBytes(bytes)
    bytes
  }

  def startObjectStore(plasmaStorePath: String, storeSuffix: String, memoryGB: Int): Unit = {
    if (storeProcess != null) return

    val memoryBytes: Long = memoryGB * 1024 * 1024 * 1024

    var i = 0
    var flag = false

    while (i < 5 && !flag) { // retry
      val currentPort: Int = ThreadLocalRandom.current.nextInt(0, 10000)
      plasmaName = s"$storeSuffix$currentPort"
      val cmd: String = s"$plasmaStorePath -m $memoryBytes -s $plasmaName"
      logger.info(cmd)
      val builder = new ProcessBuilder(cmd.split(" ").toList)
      builder.inheritIO

      try
        storeProcess = builder.start
      catch {
        case e: IOException => e.printStackTrace()
      }

      if (storeProcess != null && storeProcess.isAlive) {
        try {
          TimeUnit.MILLISECONDS.sleep(100)
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }

        flag = true
      } else {
        logger.info(s"Start object store failed, retry $i times, ...")
      }

      i += 1
    }

    if (storeProcess == null || !storeProcess.isAlive) {
      throw new RuntimeException("Start object store failed ...")
    } else {
      stopPlasmaHookTask = new Runnable {
        override def run(): Unit = {
          if (storeProcess != null && storeProcess.isAlive) {
            storeProcess.destroyForcibly
            storeProcess = null
            logger.info("plasma server has stopped!")
          }
        }
      }

      //Runtime.getRuntime.addShutdownHook(stopPlasmaHookTask)
      shutdownHookManager.addShutdownHook(stopPlasmaHookTask,
        FileSystem.SHUTDOWN_HOOK_PRIORITY + 10)
      logger.info("Start object store success")

      // load plasma_java
      logger.info("begin to load plasma_java, please make sure plasma_java in java.library.path ")
      PlasmaClient.load()
      logger.info("load plasma_java success!")
    }
  }

  def killObjectStore(): Unit = {
    //Runtime.getRuntime.removeShutdownHook(stopPlasmaHookTask)
    if (stopPlasmaHookTask != null && shutdownHookManager.hasShutdownHook(stopPlasmaHookTask)) {
      shutdownHookManager.removeShutdownHook(stopPlasmaHookTask)
      stopPlasmaHookTask = null
    }

    if (storeProcess != null && storeProcess.isAlive) {
      storeProcess.destroyForcibly
      storeProcess = null
    }
  }

  def load(): Unit = {
    try {
      System.loadLibrary("plasma_java")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def get: PlasmaClient = synchronized {
    if (clientPool.isEmpty) {
      new PlasmaClient(plasmaName, "", 0)
    } else {
      clientPool.poll()
    }
  }

  def put(client: PlasmaClient): Unit = synchronized {
    clientPool.offer(client)
  }
}
