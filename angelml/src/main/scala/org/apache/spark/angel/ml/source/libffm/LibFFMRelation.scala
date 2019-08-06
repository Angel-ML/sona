/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.angel.ml.source.libffm

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.angel.ml.feature.LabeledPoint
import org.apache.spark.angel.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.angel.ml.util.MLUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SPKSQLUtils, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

private[libffm] class LibFFMOutputWriter(
                                          path: String,
                                          dataSchema: StructType,
                                          context: TaskAttemptContext)
  extends OutputWriter {

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

  // This `asInstanceOf` is safe because it's guaranteed by `LibSVMFileFormat.verifySchema`
  private val udt = dataSchema(1).dataType.asInstanceOf[VectorUDT]

  val keyFieldMap = MetaSummary.getKeyFieldMap(dataSchema(1).metadata)

  override def write(row: InternalRow): Unit = {
    val label = row.getDouble(0)
    val vector = udt.deserialize(row.getStruct(1, udt.sqlType.length))
    writer.write(label.toString)
    vector.foreachActive { case (i, v) =>
      val field = keyFieldMap(i)
      writer.write(s" $field:$i:$v")
    }

    writer.write('\n')
  }

  override def close(): Unit = {
    writer.close()
  }
}

/** @see [[LibFFMDataSource]] for public documentation. */
// If this is moved or renamed, please update DataSource's backwardCompatibilityMap.
private[libffm] class LibFFMFileFormat
  extends TextBasedFileFormat
    with DataSourceRegister
    with Logging {

  override def shortName(): String = "libffm"

  override def toString: String = "LibFFM"

  private def verifySchema(dataSchema: StructType, forWriting: Boolean): Unit = {
    if (
      dataSchema.size != 2 ||
        !dataSchema.head.dataType.sameType(DataTypes.DoubleType) ||
        !dataSchema(1).dataType.sameType(new VectorUDT()) ||
        !(forWriting || dataSchema(1).metadata.getLong(LibFFMOptions.NUM_FEATURES) > 0)
    ) {
      throw new IOException(s"Illegal schema for libffm data, schema=$dataSchema")
    }
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val libSVMOptions = new LibFFMOptions(options)
    val paths = files.map(_.getPath.toUri.toString)
    val meta = if (libSVMOptions.isLongKey) {
      val parsed = MLUtils.parseLongKeyLibFFMFile(sparkSession, paths)
      MLUtils.computeLongKeyFFMMeta(parsed)
    } else {
      val parsed = MLUtils.parseLibFFMFile(sparkSession, paths)
      MLUtils.computeFFMMeta(parsed)
    }

    Some(
      StructType(
        StructField("label", DoubleType, nullable = false) ::
          StructField("features", new VectorUDT(), nullable = false, meta.toMetaData) :: Nil))
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema, true)
    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new LibFFMOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".libffm" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    verifySchema(dataSchema, false)
    val numFeatures = dataSchema("features").metadata.getLong(LibFFMOptions.NUM_FEATURES).toInt
    assert(numFeatures > 0)

    val libSVMOptions = new LibFFMOptions(options)
    val isSparse = libSVMOptions.isSparse

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      SPKSQLUtils.registerUDT()
      val linesReader = new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))

      val points = linesReader
        .map(_.toString.trim)
        .filterNot(line => line.isEmpty || line.startsWith("#"))
        .map { line =>
          if (libSVMOptions.isLongKey) {
            val (label, _, indices, values) = MLUtils.parseLongKeyLibFFMRecord(line)
            LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
          } else {
            val (label, _, indices, values) = MLUtils.parseLibFFMRecord(line)
            LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
          }
        }

      val converter = RowEncoder(dataSchema)
      val fullOutput = dataSchema.map { f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
      val requiredOutput = fullOutput.filter { a =>
        requiredSchema.fieldNames.contains(a.name)
      }

      val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

      points.map { pt =>
        val features = if (isSparse) pt.features.toSparse else pt.features.toDense
        requiredColumns(converter.toRow(Row(pt.label, features)))
      }
    }
  }
}
