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
import com.tencent.angel.sona.ml.param.{BooleanParam, Params}

trait HasIsCompressed extends Params {
  /**
    * Param for isCompressed.
    *
    * @group param
    */
  final val isCompressed = new BooleanParam(this, "isCompressed", "is compressed edge or not")

  final def getIsCompressed : Boolean = $(isCompressed)

  setDefault(isCompressed, false)

  final def setIsCompressed (bool: Boolean): this.type = set(isCompressed, bool)
}
