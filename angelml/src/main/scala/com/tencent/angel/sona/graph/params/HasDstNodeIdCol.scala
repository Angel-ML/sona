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

package com.tencent.angel.sona.graph.params
import com.tencent.angel.sona.ml.param.{Param, Params}

trait HasDstNodeIdCol extends Params {
  /**
    * Param for name of dst node id.
    *
    * @group param
    */
  final val dstNodeIdCol = new Param[String](this, "dstNodeIdCol", "name for dst id column")

  /** @group getParam */
  final def getDstNodeIdCol: String = $(dstNodeIdCol)

  setDefault(dstNodeIdCol, "dst")

  /** @group setParam */
  final def setDstNodeIdCol(name: String): this.type = set(dstNodeIdCol, name)
}
