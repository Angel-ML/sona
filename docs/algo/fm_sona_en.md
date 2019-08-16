# FM

## 1. Introduction
The Factorization Machine (FM) is a matrix-based machine learning algorithm proposed by Steffen Rendle. It can predict any real-valued vector. Its main advantages include: 1) capable of highly sparse data scenarios; 2) linear computational complexity.

### Factorization Model     
* Factorization Machine Model

![model](http://latex.codecogs.com/png.latex?\dpi{150}\hat{y}(x)=b+\sum_{i=1}^n{w_ix_i}+\sum_{i=1}^n\sum_{j=i+1}^n<v_i,v_j>x_ix_j)

where：![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20<v_i,v_j>) is the dot product of two k-dimensional vectors:

![dot](http://latex.codecogs.com/png.latex?\dpi{150}\inline%20<v_i,v_j>=\sum_{i=1}^kv_{i,f}\cdot%20v_{j,f})

Model parameters are：
![parameter](http://latex.codecogs.com/png.latex?\dpi{100}\inlinew_0\in%20R,w\in%20R^n,V\in%20R^{n\times%20k})
where![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20v_i)indicates that the feature i is represented by k factors, and k is the hyperparameter that determines the factorization。

### Factorization Machines as Predictors
FM can be used for a series of predictive tasks, such as：
* classification：![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20\hat{y}) can be used directly as a predictor, and the optimization criterion is to minimize the least square difference。
* regression：can use the symbol of ![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20\hat{y}) to do classification prediction，parameters are estimated by hinge function or logistic regression function at any time.。

## 2. FM on SONA
* FM algorithm model
The model of the FM algorithm consists of two parts, wide and embedding, where wide is a typical linear model. The final output is the sum of the two parts of wide and embedding.

* FM training process
    Angel implements the gradient descent method to optimize, iteratively trains the FM model, and the logic on each iteration of the worker and PS is as follows：       
    * worker：Pull the wide and embedding matrices from the PS to the local for each iteration, calculate the corresponding gradient update value, push to PS
    * PS：PS summarizes the gradient update values pushed by all workers, averages them, calculates and updates the new wide and embedding models through the optimizer.
    
* FM prediction result：
    * format：rowID,pred,prob,label
    * caption：rowID indicates the row ID of the sample, starting from 0; pred: the predicted value of the sample; prob: the probability of the sample relative to the predicted result; label: the category into which the predicted sample is classified, when the predicted result value pred is greater than 0, Label is 1, less than 0 is -1

## 3. Running and Performance
* data format
    support Libsvm and dummy two data formats, libsvm format is as follows:
    ```
    1 1:1 214:1 233:1 234:1
    ```
    
    dummy data format:
    
    ```
    1 1 214 233 234
    ```

## 4. Submitting script

Several steps must be done before editing the submitting script and running.

1. confirm Hadoop and Spark have ready in your environment
2. unzip angel-<version>-bin.zip to local directory (ANGEL_HOME)
3. upload angel-<version>-bin directory to HDFS (ANGEL_HDFS_HOME)
4. Edit $ANGEL_HOME/bin/spark-on-angel-env.sh, set SPARK_HOME, ANGEL_HOME, ANGEL_HDFS_HOME and ANGEL_VERSION

Here's an example of submitting scripts, remember to adjust the parameters and fill in the paths according to your own task.

```
#test description
actionType=train or predict
jsonFile=path-to-jsons/fm.json
modelPath=path-to-save-model
predictPath=path-to-save-predict-results
input=path-to-data
queue=your-queue

HADOOP_HOME=my-hadoop-home
source ./bin/spark-on-angel-env.sh
export HADOOP_HOME=$HADOOP_HOME

$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.instances=10 \
  --conf spark.ps.cores=2 \
  --conf spark.ps.memory=10g \
  --jars $SONA_SPARK_JARS \
  --files $jsonFile \
  --driver-memory 20g \
  --num-executors 20 \
  --executor-cores 5 \
  --executor-memory 30g \
  --queue $queue \
  --class org.apache.spark.angel.examples.JsonRunnerExamples \
  ./lib/angelml-$SONA_VERSION.jar \
  jsonFile:./fm.json \
  dataFormat:libsvm \
  data:$input \
  modelPath:$modelPath \
  predictPath:$predictPath \
  actionType:$actionType \
  numBatch:500 \
  maxIter:2 \
  lr:4.0 \
  numField:39
```