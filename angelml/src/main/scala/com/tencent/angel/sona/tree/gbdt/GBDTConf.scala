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
package com.tencent.angel.sona.tree.gbdt

import com.tencent.angel.sona.tree.basic.TParam
import com.tencent.angel.sona.tree.gbdt.helper.{FeatureImportance, LRScheduler}
import com.tencent.angel.sona.tree.gbdt.train.GBDTTrainer
import com.tencent.angel.sona.tree.gbdt.tree.GBDTParam
import com.tencent.angel.sona.tree.regression.RegTParam

object GBDTConf {
  /** -------------------- Data/Model Conf -------------------- */
  val ML_MODEL_PATH = "ml.model.path"
  val ML_TRAIN_DATA_PATH = "ml.train.path"
  val ML_VALID_DATA_PATH = "ml.valid.path"
  val ML_PREDICT_INPUT_PATH = "ml.predict.input.path"
  val ML_PREDICT_OUTPUT_PATH = "ml.predict.output.path"

  /** -------------------- ML Conf -------------------- */
  val ML_NUM_CLASS = "ml.num.class"
  val DEFAULT_ML_NUM_CLASS = GBDTParam.DEFAULT_NUM_CLASS
  val ML_NUM_FEATURE = "ml.feature.index.range"
  val ML_INSTANCE_SAMPLE_RATIO = "ml.instance.sample.ratio"
  val DEFAULT_ML_INSTANCE_SAMPLE_RATIO = TParam.DEFAULT_INS_SAMPLE_RATIO
  val ML_FEATURE_SAMPLE_RATIO = "ml.feature.sample.ratio"
  val DEFAULT_ML_FEATURE_SAMPLE_RATIO = TParam.DEFAULT_FEAT_SAMPLE_RATIO

  /** -------------------- GBDT Conf -------------------- */
  // Task Conf
  val ML_PARALLEL_MODE = "ml.gbdt.parallel.mode"
  val DEFAULT_ML_PARALLEL_MODE = GBDTTrainer.DEFAULT_PARALLEL_MODE
  val ML_TASK_TYPE = "ml.gbdt.task.type"
  val DEFAULT_ML_TASK_TYPE = "classification"
  val ML_GBDT_IMPORTANCE_TYPE = "ml.gbdt.importance.type"
  val DEFAULT_ML_GBDT_IMPORTANCE_TYPE = FeatureImportance.DEFAULT_ML_GBDT_IMPORTANCE_TYPE
  val ML_GBDT_INIT_TWO_ROUND = "ml.gbdt.init.two.round"
  // Objective Conf
  val ML_GBDT_ROUND_NUM = "ml.gbdt.round.num"
  val DEFAULT_ML_GBDT_ROUND_NUM = GBDTParam.DEFAULT_NUM_ROUND
  val ML_INIT_LEARN_RATE = "ml.learn.rate"
  val DEFAULT_ML_INIT_LEARN_RATE = GBDTParam.DEFAULT_INIT_LEARNING_RATE
  val ML_LOSS_FUNCTION = "ml.gbdt.loss.func"
  val ML_EVAL_METRIC = "ml.gbdt.eval.metric"
  val ML_REDUCE_LR_ON_PLATEAU = "ml.gbdt.reduce.lr.on.plateau"
  val DEFAULT_ML_REDUCE_LR_ON_PLATEAU = true
  val ML_REDUCE_LR_ON_PLATEAU_PATIENT = "ml.gbdt.reduce.lr.on.plateau.patient"
  val DEFAULT_ML_REDUCE_LR_ON_PLATEAU_PATIENT = LRScheduler.DEFAULT_PATIENT
  val ML_REDUCE_LR_ON_PLATEAU_THRESHOLD = "ml.gbdt.reduce.lr.on.plateau.threshold"
  val DEFAULT_ML_REDUCE_LR_ON_PLATEAU_THRESHOLD = LRScheduler.DEFAULT_THRESHOLD
  val ML_REDUCE_LR_ON_PLATEAU_DECAY_FACTOR = "ml.gbdt.reduce.lr.on.plateau.decay.factor"
  val DEFAULT_ML_REDUCE_LR_ON_PLATEAU_DECAY_FACTOR = LRScheduler.DEFAULT_DECAY_FACTOR
  val ML_REDUCE_LR_ON_PLATEAU_EARLY_STOP = "ml.gbdt.reduce.lr.on.plateau.early.stop"
  val DEFAULT_ML_REDUCE_LR_ON_PLATEAU_EARLY_STOP = LRScheduler.DEFAULT_EARLY_STOP
  val ML_GBDT_BEST_CHECKPOINT = "ml.gbdt.best.checkpoint"
  val DEFAULT_ML_GBDT_BEST_CHECKPOINT = true
  // Tree Conf
  val ML_GBDT_SPLIT_NUM = "ml.gbdt.split.num"
  val DEFAULT_ML_GBDT_SPLIT_NUM = TParam.DEFAULT_NUM_SPLIT
  val ML_GBDT_MAX_DEPTH = "ml.gbdt.tree.max.depth"
  val DEFAULT_ML_GBDT_MAX_DEPTH = TParam.DEFAULT_MAX_DEPTH
  val ML_GBDT_LEAF_WISE = "ml.gbdt.leaf.wise"
  val DEFAULT_ML_GBDT_LEAF_WISE = RegTParam.DEFAULT_LEAFWISE
  val ML_GBDT_MAX_NODE_NUM = "ml.gbdt.max.node.num"
  val DEFAULT_ML_GBDT_MAX_NODE_NUM = TParam.DEFAULT_MAX_NODE_NUM
  val ML_GBDT_MIN_CHILD_WEIGHT = "ml.gbdt.min.child.weight"
  val DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT = GBDTParam.DEFAULT_MIN_CHILD_WEIGHT
  val ML_GBDT_MIN_NODE_INSTANCE = "ml.gbdt.min.node.instance"
  val DEFAULT_ML_GBDT_MIN_NODE_INSTANCE = RegTParam.DEFAULT_MIN_NODE_INSTANCE
  val ML_GBDT_REG_ALPHA = "ml.gbdt.reg.alpha"
  val DEFAULT_ML_GBDT_REG_ALPHA = GBDTParam.DEFAULT_REG_ALPHA
  val ML_GBDT_REG_LAMBDA = "ml.gbdt.reg.lambda"
  val DEFAULT_ML_GBDT_REG_LAMBDA = GBDTParam.DEFAULT_REG_LAMBDA
  val ML_GBDT_MAX_LEAF_WEIGHT = "ml.gbdt.max.leaf.weight"
  val DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT = GBDTParam.DEFAULT_MAX_LEAF_WEIGHT
  val ML_GBDT_MIN_SPLIT_GAIN = "ml.gbdt.min.split.gain"
  val DEFAULT_ML_GBDT_MIN_SPLIT_GAIN = RegTParam.DEFAULT_MIN_SPLIT_GAIN
  // Multi-class Conf
  val ML_GBDT_MULTI_TREE = "ml.gbdt.multi.tree"
  val DEFAULT_ML_GBDT_MULTI_TREE = GBDTParam.DEFAULT_MULTI_TREE
  val ML_GBDT_FULL_HESSIAN = "ml.gbdt.full.hessian"
  val DEFAULT_ML_GBDT_FULL_HESSIAN = GBDTParam.DEFAULT_FULL_HESSIAN
}
