# GBDT on Spark on Angel

> **GBDT(Gradient Boosting Decision Tree)：梯度提升决策树** 是一种集成使用多个弱分类器（决策树）来提升分类效果的机器学习算法，在很多分类和回归的场景中，都有不错的效果。

## 1. 算法介绍

![GBDT示例](imgs/gbdt_example.png)


如图1所示，这是是对一群消费者的消费力进行预测的例子。简单来说，处理流程为：

1. 在第一棵树中，根节点选取的特征是年龄，年龄小于30的被分为左子节点，年龄大于30的被分为右叶子节点，右叶子节点的预测值为1；
2. 第一棵树的左一节点继续分裂，分裂特征是月薪，小于10K划分为左叶子节点，预测值为5；工资大于10k的划分右叶子节点，预测值为10
2. 建立完第一棵树之后，C、D和E的预测值被更新为1，A为5，B为10
3. 根据新的预测值，开始建立第二棵树，第二棵树的根节点的性别，女性预测值为0.5，男性预测值为1.5
4. 建立完第二棵树之后，将第二棵树的预测值加到每个消费者已有的预测值上，比如A的预测值为两棵树的预测值之和：5+0.5=5.5
5. 通过这种方式，不断地优化预测准确率。


## 2. 分布式训练

Spark on Angel支持两种分布式训练模式，即**数据并行**和**特征并行**。

### 数据并行

GBDT的训练方法中，核心是一种叫梯度直方图的数据结构，需要为每一个特征建立一阶梯度直方图和二阶梯度直方图。由于数据并行将训练数据按行进行切分，每个计算节点使用分配的数据集建立梯度直方图，通过网络汇总这些梯度直方图后，计算得出最佳的分裂点，如图2所示：

![数据并行GBDT](imgs/gbdt-dp.png)


### 特征并行

由于梯度直方图的大小与四个因素有关：特征数量、分裂点数量、分类数量和树节点的数量，当训练数据维度高、分类多、树深度大的时候，梯度直方图的大小较大，数据并行的训练方法有几个缺点：

1. 每个计算节点都需要存储一份完整的梯度直方图，存储的开销大。在存储空间有限时，限制了GBDT的适用性。
2. 计算节点之间需要通过网络传输本地的梯度直方图，网络通信的开销大。

为了解决数据并行的训练方式的缺点，Spark on Angel实现了特征并行的训练方式。与数据并行的训练方式不同，特征并行按列切分训练数据，训练的流程如图3所示：

![特征并行GBDT](imgs/gbdt-fp.png)

1. **数据集转换：** 由于原始的数据集一般是按行存储于分布式文件系统，我们读取训练数据后做全局的数据转换，每个计算节点分配一个特征子集。
2. **建立梯度直方图：** 每个计算节点使用特征子集建立梯度直方图，得益于特征并行的方式，不同计算节点为不同特征建立梯度直方图。
3. **寻找最佳分裂点：** 基于本地梯度直方图，每个计算节点计算出本地特征子集的最佳分裂点（分裂特征+分裂特征值）；计算节点之间通过网络汇总得到全局的最佳分裂点。
4. **计算分裂结果：** 由于每个计算节点只负责一个特征子集，训练数据的分裂结果（左子节点/右子节点）只有一个计算节点能够确定，此计算节点讲训练数据的分裂结果（经过二进制编码）广播给其他计算节点。
5. **分裂树节点：** 根据训练数据的分裂结果，更新树结构，如果没有达到停止条件，跳转到第2步继续训练。

与数据并行相比，特征并行使得每个计算节点只需要存储一部分的梯度直方图，减少存储开销，使得可以增大分裂点数量和树深度来提升模型精度。另一方面，特征并行不需要通过网络汇总梯度直方图，在高维场景下更为高效，传输分裂结果的网络开销可以通过二进制编码来降低。

## 3. 运行

###  输入格式
参数的输入格式均为key:value，如ml.feature.index.range:100代表数据集有100维特征。

> **_注意:_**  目前GBDT on Spark on Angel仅支持libsvm格式的输入数据，由于libsvm格式特征id从1开始，因此在运行时需要将特征维度+1。

### 参数

* **输入输出参数**
	* ml.train.path：训练数据的输入路径
	* ml.valid.path：验证数据的输入路径
	* ml.predict.input.path：预测数据的输入路径
	* ml.predict.output.path：预测结果的保存路径
	* ml.model.path：训练完成后模型的保存路径，或预测开始前模型的加载路径

* **任务参数**
  * ml.gbdt.task.type：任务类型，支持分类（classification）和回归（regression）
  * ml.gbdt.parallel.mode：并行类型，支持数据并行（dp）和特征并行（fp）
  * ml.gbdt.importance.type：特征重要度，训练完成后与模型一起存储，支持特征分裂总次数（weight）、特征平均分裂增益（gain）和特征总分裂增益（total_gain），默认total_gain
  * ml.num.class：分类数量，仅对分类任务有用
  * ml.feature.index.range：数据集特征维度
  * ml.instance.sample.ratio：样本采样比例（0到1之间），默认不采样
  * ml.feature.sample.ratio：特征采样比例（0到1之间），默认不采样
* **优化参数**
  * ml.gbdt.round.num：训练轮次，默认20轮
  * ml.learn.rate：学习速率，默认0.1
  * ml.gbdt.loss.func：代价函数，分类任务支持二分类（binary:logistic）、多分类（multi:logistic）和均方根误差（rmse），回归任务固定为均方根误差（rmse）
  * ml.gbdt.eval.metric：模型指标，支持rmse、error、log-loss、cross-entropy、precision和auc，以逗号分隔
  * ml.gbdt.reduce.lr.on.plateau：是否当验证集上的指标连续多轮无明显提升时下降学习速率，true或false，默认true
  * ml.gbdt.reduce.lr.on.plateau.patient：指标无明显提升的轮数，默认5轮
  * ml.gbdt.reduce.lr.on.plateau.threshold：指标明显提升的阈值，默认0.0001
  * ml.gbdt.reduce.lr.on.plateau.decay.factor：学习速率下降的比例，默认0.1
  * ml.gbdt.reduce.lr.on.plateau.early.stop：当多次学习速率下降后指标仍然无明显提升时，提前结束训练，默认为3次，（若取-1，则不会提前结束训练）
  * ml.gbdt.best.checkpoint：是否仅存储在验证集上达到最优指标的模型（对二分类任务，指标为log-loss，对多分类任务，指标为cross-entropy，对回归任务，指标为rmse），true或false，默认true
* **决策树参数**
  * ml.gbdt.split.num：每个特征的分裂点的数量，默认20
  * ml.gbdt.tree.max.depth：树的最大深度，默认6
  * ml.gbdt.leaf.wise：是否采用深度优先方法训练一棵树，否则采用逐层方法，true或false，默认false
  * ml.gbdt.max.node.num：每棵树的最大树节点数量，默认127（即深度为6的完全树）
  * ml.gbdt.min.child.weight：分裂树节点后子节点的最小hessian值，默认0
  * ml.gbdt.min.node.instance：可分裂树节点上数据的最少样本数量，默认1024
  * ml.gbdt.reg.alpha：正则化系数，默认0
  * ml.gbdt.reg.lambda：正则化系统，默认1
  * ml.gbdt.max.leaf.weight：叶子节点的最大预测值（绝对值），若为0则不限制预测值，默认0
  * ml.gbdt.min.split.gain：分裂树节点需要的最小增益，默认0
* **多分类任务参数**
  * ml.gbdt.multi.tree：是否使用一轮多棵树（即多个one-vs-rest二分类器），否则一轮一棵树（即单个多分类器），仅当多分类任务时有效，true或false，默认false
  * ml.gbdt.full.hessian：是否使用全量hessian矩阵计算，否则使用对角近似hessian矩阵，仅当一轮一棵树时有效，true或false，默认false

> **_注意:_**  使用全量hessian矩阵要求存储所有训练样本的hessian矩阵，需要较大的存储空间，而且会造成较高的计算开销；同时实验表明，使用对角近似矩阵的准确率与使用全量矩阵相近或更高。因此，除非多分类类别数量较少且类别之间有强相互关系，否则请不要使用全量矩阵进行计算。

### 训练任务启动命令示例

使用spark提交任务

```shell
./spark-submit \
    --master yarn --deploy-mode cluster \
    --name "GBDT on Spark-on-Angel" \
    --queue $queue \
    --driver-memory 5g \  
    --num-executors 10 \  
    --executor-cores 1 \  
    --executor-memory 10g \   
    --class org.apache.spark.angel.ml.tree.gbdt.predict.GBDTPredictor \  
    angelml-${SONA_VERSION}.jar \   
    ml.train.path:XXX ml.valid.path:XXX ml.model.path:XXX \  
    ml.gbdt.parallel.mode:fp \ 
    ml.gbdt.importance.type:total_gain \
    ml.gbdt.task.type:classification \
    ml.gbdt.loss.func:binary:logistic ml.gbdt.eval.metric:log-loss,error,auc \  
    ml.num.class:2 \
    ml.feature.index.range:100 \ 
    ml.instance.sample.ratio:0.8 \ 
    ml.feature.sample.ratio:0.8 \ 
    ml.gbdt.round.num:100 \
    ml.learn.rate:0.05    
```

### 预测任务启动命令示例

使用spark提交任务

```shell
./spark-submit \
      --master yarn --deploy-mode cluster \ 
      --name "GBDT on Spark-on-Angel" \
      --queue $queue \
      --driver-memory 5g \  
      --num-executors 10 \  
      --executor-cores 1 \  
      --executor-memory 10g \   
      --class org.apache.spark.angel.ml.tree.gbdt.predict.GBDTPredictor \  
      angelml-${SONA_VERSION}.jar \
      ml.model.path:XXX ml.predict.input.path:XXX ml.predict.output.path:XXX
```



