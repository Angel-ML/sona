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

package org.apache.spark.angel.ml.source.libsvm

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

private[libsvm] class LibSVMOutputWriter(
                                          path: String,
                                          dataSchema: StructType,
                                          context: TaskAttemptContext)
  extends OutputWriter {

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

  // This `asInstanceOf` is safe because it's guaranteed by `LibSVMFileFormat.verifySchema`
  private val udt = dataSchema(1).dataType.asInstanceOf[VectorUDT]

  override def write(row: InternalRow): Unit = {
    val label = row.getDouble(0)
    val vector = udt.deserialize(row.getStruct(1, udt.sqlType.length))
    writer.write(label.toString)
    vector.foreachActive { case (i, v) =>
      writer.write(s" ${i + 1}:$v")
    }

    writer.write('\n')
  }

  override def close(): Unit = {
    writer.close()
  }
}

/** @see [[LibSVMDataSource]] for public documentation. */
// If this is moved or renamed, please update DataSource's backwardCompatibilityMap.
private[libsvm] class LibSVMFileFormat
  extends TextBasedFileFormat
    with DataSourceRegister
    with Logging {

  override def shortName(): String = "libsvmex"

  override def toString: String = "LibSVMEX"

  private def verifySchema(dataSchema: StructType, forWriting: Boolean): Unit = {
    if (
      dataSchema.size != 2 ||
        !dataSchema.head.dataType.sameType(DataTypes.DoubleType) ||
        !dataSchema(1).dataType.sameType(new VectorUDT()) ||
        !(forWriting || dataSchema(1).metadata.getLong(LibSVMOptions.NUM_FEATURES) > 0) ||
        !(forWriting || dataSchema(1).metadata.getLong(LibSVMOptions.VALIDATE_FEATURE_NUMBER) >= -1)
    )
    {
      throw new IOException(s"Illegal schema for libsvm data, schema=$dataSchema")
    }
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val libSVMOptions = new LibSVMOptions(options)

    require(files.nonEmpty, "No input path specified for libsvm data")
    val numFeatures: Long = libSVMOptions.numFeatures.getOrElse {
      logWarning(
        "'numFeatures' option not specified, determining the number of features by going " +
          "though the input. If you know the number in advance, please specify it via " +
          "'numFeatures' option to avoid the extra scan.")

      val paths = files.map(_.getPath.toUri.toString)
      if (libSVMOptions.isLongKey) {
        val parsed = MLUtils.parseLongKeyLibSVMFile(sparkSession, paths)
        MLUtils.computeLongKeyNumFeatures(parsed)
      } else {
        val parsed = MLUtils.parseLibSVMFile(sparkSession, paths)
        MLUtils.computeNumFeatures(parsed)
      }
    }

    val validateFeatureNumber: Long = libSVMOptions.numValidateFeatures.getOrElse{
      if (libSVMOptions.isSparse){
        logWarning(
          "'numValidateFeatures' option not specified, determining the number of features by going " +
            "though the input. If you know the number in advance, please specify it via " +
            "'numValidateFeatures' option to avoid the extra scan.")

        -1L
      } else {
        numFeatures
      }
    }

    val keyType: String = options.getOrElse(LibSVMOptions.KEY_TYPE, LibSVMOptions.INT_KEY_TYPE)

    val featuresMetadata = new MetadataBuilder()
      .putLong(LibSVMOptions.NUM_FEATURES, numFeatures)
      .putLong(LibSVMOptions.VALIDATE_FEATURE_NUMBER, validateFeatureNumber)
      .putString(LibSVMOptions.KEY_TYPE, keyType)
      .build()

    Some(
      StructType(
        StructField("label", DoubleType, nullable = false) ::
          StructField("features", new VectorUDT(), nullable = false, featuresMetadata) :: Nil))
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
        new LibSVMOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".libsvm" + CodecStreams.getCompressionExtension(context)
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
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    verifySchema(dataSchema, false)
    val numFeatures = dataSchema("features").metadata.getLong(LibSVMOptions.NUM_FEATURES).toInt
    assert(numFeatures > 0)

    val libSVMOptions = new LibSVMOptions(options)
    val isSparse = libSVMOptions.isSparse

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    file: PartitionedFile => {

      SPKSQLUtils.registerUDT()

      val linesReader = new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))

      val points = linesReader
        .map(_.toString.trim)
        .filterNot(line => line.isEmpty || line.startsWith("#"))
        .map { line =>
          if (libSVMOptions.isLongKey) {
            val (label, indices, values) = MLUtils.parseLongKeyLibSVMRecord(line)
            LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
          } else {
            val (label, indices, values) = MLUtils.parseLibSVMRecord(line)
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
