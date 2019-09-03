package org.apache.spark.util


import java.io.{File, IOException}
import java.nio.file.{Files, Paths}
import java.util.{Random, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try


object SparkUtil extends Logging {
  val random = new Random()
  private val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  /**
    * Define a default value for driver memory here since this value is referenced across the code
    * base and nearly all files already use Utils.scala
    */
  val DEFAULT_DRIVER_MEM_MB: Int = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt

  /**
    * The performance overhead of creating and logging strings for wide schemas can be large. To
    * limit the impact, we bound the number of fields to include by default. This can be overridden
    * by setting the 'spark.debug.maxToStringFields' conf in SparkEnv.
    */
  val DEFAULT_MAX_TO_STRING_FIELDS = 25

  val SPARK_YARN_CREDS_TEMP_EXTENSION = ".tmp"
  val SPARK_YARN_CREDS_COUNTER_DELIM = "-"

  /**
    * Number of records to update input metrics when reading from HadoopRDDs.
    *
    * Each update is potentially expensive because we need to use reflection to access the
    * Hadoop FileSystem API of interest (only available in 2.5), so we should do this sparingly.
    */
  val UPDATE_INPUT_METRICS_INTERVAL_RECORDS = 1000

  def majorMinorVersion(sparkVersion: String): (Int, Int) = {
    majorMinorRegex.findFirstMatchIn(sparkVersion) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Spark tried to parse '$sparkVersion' as a Spark" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    // scalastyle:off classforname
    Try {
      Class.forName(clazz, false, getContextOrSparkClassLoader)
    }.isSuccess
    // scalastyle:on classforname
  }

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
   * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
   * so function return (x % mod) + mod in that case.
   */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  def makeBinarySearch[K: Ordering : ClassTag]: (Array[K], K) => Int = {
    CollectionsUtils.makeBinarySearch
  }

  def sketch[K: ClassTag](rdd: RDD[K],
                          sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    RangePartitioner.sketch(rdd, sampleSizePerPartition)
  }

  def determineBounds[K: Ordering : ClassTag](
                                               candidates: ArrayBuffer[(K, Float)],
                                               partitions: Int): Array[K] = {
    RangePartitioner.determineBounds(candidates, partitions)
  }

  def getXORShiftRandom(seed: Long): XORShiftRandom = new XORShiftRandom(seed)

  def createOpenHashMap[K: ClassTag, V: ClassTag](): OpenHashMap[K, V] = {
    new OpenHashMap[K, V]()
  }

  def createOpenHashSet[K: ClassTag](): OpenHashSet[K] = {
    new OpenHashSet[K]()
  }

  /**
    * Returns a Configuration object with Spark configuration applied on top. Unlike
    * the instance method, this will always return a Configuration instance, and not a
    * cluster manager-specific type.
    */
  def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    appendS3AndSparkHadoopConfigurations(conf, hadoopConf)
    hadoopConf
  }

  private def appendS3AndSparkHadoopConfigurations(
                                                    conf: SparkConf,
                                                    hadoopConf: Configuration): Unit = {
    // Note: this null check is around more than just access to the "conf" object to maintain
    // the behavior of the old implementation of this code, for backwards compatibility.
    if (conf != null) {
      // Explicitly check for S3 environment variables
      val keyId = System.getenv("AWS_ACCESS_KEY_ID")
      val accessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      if (keyId != null && accessKey != null) {
        hadoopConf.set("fs.s3.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3n.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3a.access.key", keyId)
        hadoopConf.set("fs.s3.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3n.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3a.secret.key", accessKey)

        val sessionToken = System.getenv("AWS_SESSION_TOKEN")
        if (sessionToken != null) {
          hadoopConf.set("fs.s3a.session.token", sessionToken)
        }
      }
      appendSparkHadoopConfigs(conf, hadoopConf)
      val bufferSize = conf.get("spark.buffer.size", "65536")
      hadoopConf.set("io.file.buffer.size", bufferSize)
    }
  }

  private def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Copy any "spark.hadoop.foo=bar" spark properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
  }

  def getFSBytesWrittenOnThreadCallback: () => Long = {
    val threadStats = FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics)
    val f = () => threadStats.map(_.getBytesWritten).sum
    val baselineBytesWritten = f()
    () => f() - baselineBytesWritten
  }

  def parse(args: Array[String]): Map[String, String] = {
    val cmdArgs = new mutable.HashMap[String, String]()
    println("parsing parameter")
    for (arg <- args) {
      val sepIdx = arg.indexOf(":")
      if (sepIdx != -1) {
        val k = arg.substring(0, sepIdx).trim
        val v = arg.substring(sepIdx + 1).trim
        if (v != "" && v != "Nan" && v != null) {
          cmdArgs.put(k, v)
          println(s"param $k = $v")
        }
      }
    }
    cmdArgs.toMap
  }

  def UDTRegister(userClass: String, udtClass: String): Unit = {
    UDTRegistration.register(userClass, udtClass)
  }

  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    */
  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
    * Delete a file or directory and its contents recursively.
    * Don't follow directories if they are symlinks.
    * Throws an exception if deletion is unsuccessful.
    */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          ShutdownHookManager.removeShutdownDeleteDir(file)
        }
      } finally {
        if (file.delete()) {
          logTrace(s"${file.getAbsolutePath} has been deleted")
        } else {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  /**
    * Check to see if file is a symbolic link.
    */
  def isSymlink(file: File): Boolean = Files.isSymbolicLink(Paths.get(file.toURI))


  def getSparkSession(sparkContext: SparkContext): SparkSession = {
    SparkSession.builder()
      .sparkContext(sparkContext)
      .getOrCreate()
  }
}