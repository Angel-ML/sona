# GBDT on Spark on Angel

> **GBDT(Gradient Boosting Decision Tree)** is an ensemble machine learning algorithm that trains a set of regression trees.

## 1. Introduction of GBDT

![GBDT example](imgs/gbdt_example.png)

The above figure is an example of predicting the consumption of consumers.

1. In the first tree, the root node use the feature of age. 
Consumers older than 30 are classified to the left tree node, while those younger than 30 is classified to the right node.
The prediction of the right node is 1.
2. Then, the left tree node is splited. The split feature is salary.
3. Once the first tree is built, the predictions of C, D and E are updated to 1, 5, and 10.
4. Build the second tree with new predictions. Update the predictions of consumers by adding the predictions of the second tree.
5. This process iterates until satisfying the stopping criteria.

## 2. Distributed training

Spark on Angel supports two modes of distributed training, **data parallelism** and **feature parallelism**.

### Data parallelism

The core data structure of training GBDT is called gradient histogram.
GBDT bulild one first-order gradient histogram and one second-order gradient histogram for each feature.
Since data parallelism partitions the training data by row, every worker builds gradient histogram using a data subset.
Then, it finds the best split result via merging local gradient histograms through the network.

![Data parallelism of GBDT](imgs/gbdt-dp.png)

### Feature parallelism

Since the size of gradient histogram is affected by four factors: the number of features, the number of splits, the number of classes, and the number of tree nodes.
For high-dimensional features, large classes and deep trees, the size of gradient histogram can be very large,
causing several problems for data parallelism.
 
1. Expensive memory cost. Every worker needs to store a whole copy of gradient histograms.
2. Expensive communication cost. Workers need to exchange local gradient histograms through the network.

To address the drawbacks of data parallelism, Spark on Angel implements feature parallelism.
Different from data parallelism, the training process is as follows.

![Feature parallelism of GBDT](imgs/gbdt-fp.png)

1. **Data transformation.** 
Since the original datasets are generally stored in distributed file systems by rows, SONA loads the dataset and transforms to feature subsets.
2. **Build gradient histogram.**
Every worker builds gradient histograms for a feature subset.
3. **Find the best split.** 
Each worker calculates the best split (split feature and split value) with local gradient histograms.
Workers get the global best split by exchanging local best splits.
4. **Calculate split result.**
Since each worker only stores a feature subset, the split result of training data is only known by one worker.
This worker broadcasts the split result (binary format) to other workers.
5. **Split tree node**.
Each worker splits tree node and updates tree structure.

Compared with data parallelism, feature parallelism makes each worker build gradient histograms for a feature subset, the memory cost is reduced.
Besides, feature parallelism does not need to merge gradient histogram through the network, thus the communication cost is reduced.

## 3. Running

###  Input Format
To submit a job, the format of passing parameters should be "key:value". For instance, "ml.feature.index.range:100" specifies the dimensionality of training dataset to 100.

> **_Note:_**  Currently the only supported data format of GBDT on Spark on Angel is the libsvm format, whose feature id starts from 1, thus the dimensionality should be +1 when submitting a job.

### Parameters

* **I/O Parameters**
  * **ml.train.path** Input path of data for training
  * **ml.valid.path** Input path of data for validation
  * **ml.predict.input.path** Input path of data for prediction
  * **ml.predict.output.path** Output path of prediction
  * **ml.model.path** The path to save a model after training, or to load a model before prediction

* **Task Parameters**
  * **ml.gbdt.task.type** Type of the task - "classification" or "regression"
  * **ml.gbdt.parallel.mode** Parallel model - data parallel ("dp") or feature parallel ("fp") 
    * dp: data parallel
    * fp: feature parallel
  * **ml.gbdt.importance.type** Type of feature importance, which will be saved together with model after training
    * weight: the number of times a feature is used to split tree nodes
    * gain: the average gain a feature is used to split tree nodes
    * total_gain:  the total gain a feature is used to split tree nodes
  * **ml.num.class** Number of classes in classification task
  * **ml.feature.index.range** Dataset dimensionality
  * **ml.instance.sample.ratio** Instance sampling ratio between 0 and 1 (*default* 1)
  * **ml.feature.sample.ratio** Feature sampling ratio between 0 and 1 (*default* 1)
* **Optimization/Objective Parameters**
  * **ml.gbdt.round.num** Number of training rounds (*default* 20)
  * **ml.learn.rate** Learning rate (*default* 0.1)
  * **ml.gbdt.loss.func** Loss function. For classification task, "binary:logistic", "multi:logistic", and "rmse" are supported. For regression task, only "rmse" is supported.
  * **ml.gbdt.eval.metric** Model evaluation metric. "rmse", "error", "log-loss", "cross-entropy", "precision", and "auc" are supported. Separate by commas when passing multiple metrics
  * **ml.gbdt.reduce.lr.on.plateau** Whether to reduce the learning rate when the metric on validation set does not improve for several rounds (*default* true)
  * **ml.gbdt.reduce.lr.on.plateau.patient** Number of rounds indicating metric improvement (*default* 5)
  * **ml.gbdt.reduce.lr.on.plateau.threshold** Threshold indicating metric improvement (*default* 0.0001)
  * **ml.gbdt.reduce.lr.on.plateau.decay.factor** Decay factor for learning rate reduction (*default* 0.1)
  * **ml.gbdt.reduce.lr.on.plateau.early.stop** Early stop the training when the metric on validation set does not improve for several times of learning rate reduction (*default* 3, set to -1 to prohibit early stopping)
  * **ml.gbdt.best.checkpoint** Whether to save the model checkpoint which achieves best metric on validation set (*default* true). For binary-classification task, the metric is log-loss; for multi-classification task, the metric is cross-entropy; for regression task, the metric is rmse.
* **Decision Tree Parameters**
  * **ml.gbdt.split.num** The number of candidate splits for each feature (*default* 20)
  * **ml.gbdt.tree.max.depth** The maximum depth of each tree (*default* 6)
  * **ml.gbdt.leaf.wise** Whether to use leaf-wise strategy to train a tree, otherwise level-wise strategy will be used (*default* true)
  * **ml.gbdt.max.node.num** The maximum number of tree nodes of each tree (*default* 127, i.e. complete tree with depth of 6)
  * **ml.gbdt.min.child.weight** The minimum hessian value of child node after splitting (*default* 0)
  * **ml.gbdt.min.node.instance** The minimum number of training instances of a tree node (*default* 1024)
  * **ml.gbdt.reg.alpha** L1 regularization term on weights (*default* 0)
  * **ml.gbdt.reg.lambda** L2 regularization term on weights (*default* 1)
  * **ml.gbdt.max.leaf.weight** Maximum (absolute) prediction value on tree leaves (*default* 0, set to 0 to prohibit the restriction of prediction value)
  * **ml.gbdt.min.split.gain** Minimum gain to split (*default* 0)
* **Multi-classification Task Parameters**
  * **ml.gbdt.multi.tree** Whether to train multiple trees per round (i.e., multiple one-vs-rest classifier), otherwise one tree per round will be trained (i.e., one multi-class classifier). Only valid for multi-classification task (*default* false)
  * **ml.gbdt.full.hessian** Whether to train with full hessian matrices, otherwise diagonal hessian matrices will be used. Only valid for multi-classification task and "ml.gbdt.multi.tree" is false (*default* false)

> **_Note:_**  Training with full hessian matrices requires to store the hessian matrices of all training instances, which will lead to high memory consumption and computation overhead. While training with diagoal hessian matrices can empirically achieve comparable or even higher accuracy. Therefore, unless the number of classes is small and strong inter-class relationship exists, please do NOT use full hessian matrices.

### Submitting Scripts
after install sona(see: [sona quick start](./tutorials/sona_quick_start.md)), then ```cd sona-${SONA_VERSION}-bin```, create a script as follows to submit a job:

To submit a training job:

```shell
source ./bin/spark-on-angel-env.sh

$SPARK_HOME/bin/spark-submit \
    --master yarn --deploy-mode cluster \
    --name "GBDT on Spark-on-Angel" \
    --queue $queue \
    --driver-memory 1g \
    --num-executors 1 \
    --executor-cores 1 \
    --executor-memory 1g \
    --class com.tencent.angel.sona.tree.gbdt.train.GBDTTrainer \
    angelml-${SONA_VERSION}.jar \
    ml.train.path:hdfs://.../agaricus/agaricus_127d_train.libsvm \
    ml.valid.path:hdfs://.../agaricus/agaricus_127d_train.libsvm \
    ml.model.path:XXX \
    ml.gbdt.parallel.mode:fp \
    ml.gbdt.importance.type:total_gain \
    ml.gbdt.task.type:classification \
    ml.gbdt.loss.func:binary:logistic \
    ml.gbdt.eval.metric:log-loss,error,auc \
    ml.num.class:2 \
    ml.feature.index.range:128 \
    ml.instance.sample.ratio:0.8 \
    ml.feature.sample.ratio:0.8 \
    ml.gbdt.round.num:10 \
    ml.learn.rate:0.05
```

To submit a prediction job:

```shell
source ./bin/spark-on-angel-env.sh

$SPARK_HOME/bin/spark-submit \
      --master yarn --deploy-mode cluster \
      --name "GBDT on Spark-on-Angel" \
      --queue $queue \
      --driver-memory 1g \
      --num-executors 1 \
      --executor-cores 1 \
      --executor-memory 1g \
      --class com.tencent.angel.sona.tree.gbdt.predict.GBDTPredictor \
      angelml-${SONA_VERSION}.jar \
      ml.model.path:XXX ml.predict.input.path:XXX ml.predict.output.path:XXX
```

**notice:** the resource such as: driver-memory, num-executors, executor-cores and executor-memory dependence on your data size, please adjust them when you change the dataset.
