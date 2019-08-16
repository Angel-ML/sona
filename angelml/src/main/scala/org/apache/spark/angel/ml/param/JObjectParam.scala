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
package org.apache.spark.angel.ml.param

import org.apache.spark.angel.ml.util.Identifiable
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class JObjectParam(parent: String, name: String, doc: String, isValid: JObject => Boolean)
  extends Param[JObject](parent, name, doc, isValid) {
  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, (value: JObject) => value != null)

  def this(parent: Identifiable, name: String, doc: String, isValid: JObject => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  override def w(value: JObject): ParamPair[JObject] = super.w(value)

  override def jsonEncode(value: JObject): String = {
    compact(render(value))
  }

  override def jsonDecode(json: String): JObject = {
    implicit val formats: DefaultFormats = DefaultFormats
    parse(json).asInstanceOf[JObject]
  }
}
