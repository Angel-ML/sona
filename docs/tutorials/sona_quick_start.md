# SONA(Spark on Angel) Quick Start 

he SONA job is essentially a Spark Application with an associated Angel-PS application. 
After the job is successfully submitted, there will be two separate Applications on the cluster, 
one is the Spark Application and the other is the Angel-PS Application. The two Applications are not coupled. 
If the SONA job is deleted, users are required to kill both the Spark and Angel-PS Applications manually.

## Deployment Process

1. **Install Spark**
2. **Install SONA**
	1. unzip sona-\<version\>-bin.zip
	2. set these environmental variables: `SPARK_HOME`, `SONA_HOME`, `SONA_HDFS_HOME` in sona-\<version\>-bin/bin/spark-on-angl-env.sh
	3. put the extracted folder `sona-\<version\>-bin` into `SONA_HDFS_HOME`

3. Configure Environment Variables

	- need to import environment script：source ./spark-on-angel-env.sh
	- configure the Jar package location：spark.ps.jars=\$SONA_ANGEL_JARS和--jars \$SONA_SPARK_JARS

## Submit Task

After completing sona program coding, then package it. At last, use `spark-submit` script to submit the task.


## Run Example（Logistic Regression）

```bash
#! /bin/bash
- cd sona-<version>-bin/bin; 
- ./SONA-example
```

script as follows：

```bash
#!/bin/bash

source ./spark-on-angel-env.sh

$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --jars $SONA_SPARK_JARS\
    --name "LR-spark-on-angel" \
    --files <logreg.json path> \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class org.apache.spark.angel.examples.JsonRunnerExamples \
    ./../lib/angelml-${SONA_VERSION}.jar \
    data:<input_path> \
    modelPath:<output_path> \
    jsonFile:./logreg.json \
    lr:0.1
```

> Attention: the parameters of Angel PS need to be set：`spark.ps.instance`，`spark.ps.cores`，`spark.ps.memory`
> ```--files <logreg.json  path>``` using this parameter to upload your local json file, here ```<logreg.json path>```is the local path of json(such as: xx/xx/logreg.json)
> ```jsonFile:./logreg.json \``` this parameter is using the json you upload
> resources such as: executor, driver, ps, depend on your dataset


## LR Json Example 

- [detail json](https://github.com/Angel-ML/angel/blob/master/docs/basic/json_conf_en.md)
- [data](https://github.com/Angel-ML/angel/tree/master/data/a9a/a9a_123d_train.libsvm)

```json
{
  "data": {
    "format": "libsvm",
    "indexrange": 123,
    "validateratio": 0.1,
    "sampleratio": 1.0
  },
  "train": {
    "epoch": 10,
    "lr": 0.5
  },
  "model": {
    "modeltype": "T_DENSE_SPARSE"
  },
  "default_optimizer": {
    "type": "momentum",
    "momentum": 0.9,
    "reg2": 0.001
  },
  "layers": [
    {
      "name": "wide",
      "type": "simpleinputlayer",
      "outputdim": 1,
      "transfunc": "identity"
    },
    {
      "name": "simplelosslayer",
      "type": "simplelosslayer",
      "lossfunc": "logloss",
      "inputlayer": "wide"
    }
  ]
}

```

Users are encouraged to program instead of just using bash script. here is an example: 

```scala
import com.tencent.angel.sona.core.DriverContext
import org.apache.spark.angel.ml.classification.AngelClassifier
import org.apache.spark.angel.ml.feature.LabeledPoint
import org.apache.spark.angel.ml.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

val spark = SparkSession.builder()
  .master("yarn-cluster")
  .appName("AngelClassification")
  .getOrCreate()

val sparkConf = spark.sparkContext.getConf
val driverCtx = DriverContext.get(sparkConf)

driverCtx.startAngelAndPSAgent()

val libsvm = spark.read.format("libsvmex")
val dummy = spark.read.format("dummy")

val trainData = libsvm.load("./data/angel/a9a/a9a_123d_train.libsvm")

val classifier = new AngelClassifier()
  .setModelJsonFile("./angelml/src/test/jsons/logreg.json")
  .setNumClass(2)
  .setNumBatch(10)
  .setMaxIter(2)
  .setLearningRate(0.1)

val model = classifier.fit(trainData)

model.write.overwrite().save("trained_models/lr")
```