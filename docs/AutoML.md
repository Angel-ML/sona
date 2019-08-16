## AutoML

### Hyper-parameters of AngelClassifier
The following hyper-parameters can be automatically tuned with [Angel-AutoML](https://github.com/Angel-ML/automl), 
the AutoML module of Angel.
- numBatch
- maxIter
- learningRate
- decayAlpha
- decayBeta
- decayIntervals

### Hyper-parameters of Tuner
- ml.auto.tuner.iter. The maximal iterations of tuning.
- ml.auto.tuner.model. Can be "Random", "Grid", or "GaussianProcess".
- ml.auto.tuner.params. 
  - Supported format: "PARAM_NAME|PARAM_TYPE|VALUE_TYPE|PARAM_RANGE", multiple hyper-parameters are separated by #.
  - Param type should be D or C (D means discrete, C means continuous)
  - value type should be float, double, int or long.
  - For the format of param range, please refer to [Angel-AutoML](https://github.com/Angel-ML/automl/blob/master/README.md).
  - Example: ml.learn.rate|C|double|0.1:1:100#ml.learn.decay|D|float|0,0.01,0.1
  
### The submit scripts
The following is a submit script for tuning machine learning algorithms of Angel.
```bash
#! /bin/bash
source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --jars $SONA_SPARK_JARS \
    --name "tuner-spark-on-angel" \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class org.apache.spark.angel.examples.AutoJsonRunnerExample \
    ./lib/angelml-${ANGEL_VERSION}.jar
    actionType:train data:path/to/data dataFormat:libsvm jsonFile:./fm.json modelPath:/path/to/model \
    numClasses:2 numField:13 numBatch:10 maxIter:10 learningRate:0.1 decayAlpha:0.001 decayBeta:0.001 decayIntervals:10 \
    ml.auto.tuner.iter:10 ml.auto.tuner.model:GaussianProcess ml.auto.tuner.params:"learningRate|C|double|0.1:1:100#maxIter|D|float|1:5:1"
```