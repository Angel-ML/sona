/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.sona.ml.source.libffm

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import scala.collection.mutable

class MetaSummary extends Serializable {
  val fieldSet = new mutable.HashSet[Long]()
  val keyFieldMap = new mutable.HashMap[Long, Long]()
  private var numFeatures: Long = 0L
  private var numSamples: Long = 0L

  def getNumFeatures: Long = numFeatures + 1

  def getNumActives: Long = keyFieldMap.size.toLong

  def getNumSamples: Long = numSamples

  def getNumFields: Long = fieldSet.size.toLong

  def addInt(sample: (Double, Array[Long], Array[Int], Array[Double])): this.type = {
    val (_, fields, keys, _) = sample

    fields.zip(keys).foreach { case (field, key) =>
      fieldSet.add(field)
      keyFieldMap.put(key.toLong, field)

      if (numFeatures < key) {
        numFeatures = key
      }
    }

    numSamples += 1

    this
  }

  def addLong(sample: (Double, Array[Long], Array[Long], Array[Double])): this.type = {
    val (_, fields, keys, _) = sample

    fields.zip(keys).foreach { case (field, key) =>
      fieldSet.add(field)
      keyFieldMap.put(key, field)

      if (numFeatures < key) {
        numFeatures = key
      }
    }

    numSamples += 1

    this
  }

  def merge(other: MetaSummary): this.type = {
    fieldSet ++= other.fieldSet
    keyFieldMap ++= other.keyFieldMap
    numFeatures = Math.max(numFeatures, other.numFeatures)
    numSamples += other.numSamples

    this
  }

  def toMetaData: Metadata = {
    val (keys, fields) = keyFieldMap.toArray.unzip
    new MetadataBuilder()
      .putLong(LibFFMOptions.NUM_FEATURES, getNumFeatures)
      .putLong(MetaSummary.numActives, getNumActives)
      .putLong(LibFFMOptions.NUM_FIELDS, getNumFields)
      .putLongArray(MetaSummary.fieldSetName, fieldSet.toArray)
      .putLongArray(MetaSummary.keyName, keys)
      .putLongArray(MetaSummary.fieldName, fields)
      .build()
  }
}

object MetaSummary {
  val fieldSetName = "fieldSet"
  val keyName = "keyName"
  val fieldName = "fieldName"
  val numActives = "numActives"

  def addInt(partition: Iterator[(Double, Array[Int], Array[Int], Array[Double])]): Iterator[MetaSummary] = {
    val meta = new MetaSummary
    partition.foreach { case (_, fields, keys, _) =>
      fields.zip(keys).foreach { case (field, key) =>
        meta.fieldSet.add(field)
        meta.keyFieldMap.put(key.toLong, field)

        if (meta.numFeatures < key) {
          meta.numFeatures = key
        }
      }

      meta.numSamples += 1
    }

    Seq(meta).toIterator
  }

  def addLong(partition: Iterator[(Double, Array[Int], Array[Long], Array[Double])]): Iterator[MetaSummary] = {
    val meta = new MetaSummary
    partition.foreach { case (_, fields, keys, _) =>
      fields.zip(keys).foreach { case (field, key) =>
        meta.fieldSet.add(field)
        meta.keyFieldMap.put(key, field)

        if (meta.numFeatures < key) {
          meta.numFeatures = key
        }
      }

      meta.numSamples += 1
    }

    Iterator.single(meta)
  }

  def getFieldSet(meta: Metadata): Set[Int] = {
    try {
      meta.getLongArray(fieldSetName).map(_.toInt).toSet
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
      case ae: AssertionError =>
        ae.printStackTrace()
        null
    }
  }

  def getKeyFieldMap(meta: Metadata): Map[Long, Int] = {
    try {
      val keys = meta.getLongArray(keyName)
      val fields = meta.getLongArray(fieldName).map(_.toInt)

      keys.zip(fields).toMap
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
      case ae: AssertionError =>
        ae.printStackTrace()
        null
    }

  }
}

