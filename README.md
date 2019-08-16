# SONA Overview
Spark On Angel (SONA), arming Spark with a powerful Parameter Server, which enable Spark to train very big models

Similar to Spark MLlib, Spark on Angel is a standalone machine learning library built on Spark (yet it does not rely on Spark MLlib, Figure 1). 
SONA was based on RDD APIs and only included model training step in previous versions. In Angel 3.0, we introduce various new features to SONA:
- Integration of feature engineering into SONA. Instead of simply borrowing Spark’s feature engineering operators, we add support for long index vector to all the operators to enable training of high dimensional sparse models. 
- Seamless connection with automatic hyperparameter tuning.
- Spark-fashion APIs that introduce no cost for Spark users to switch to Angel.
- Support for two new data formats: LibFFM and Dummy.

| ![sona_fig00](docs/imgs/sona_fig00.png) |
|  :----:    |
| *Figure 1: SONA is a another machine learning & graph library on Spark Core*   |

Figure 2 demonstrate the run time architecture of SONA.

| ![sona_fig01](docs/imgs/sona_fig01.png) |
|  :----:    |
| *Figure 2: Architecture of SONA*   |

- There is a `AngelClient` on Spark driver. `AngelClient` is used to start Angel parameter server, create, load, initial and save matrix of the model. 
- There is a `PSClient/PSAgent` on Spark executor. Algorithms can pull parameter and push gradient through `PSAgent`
- The Angel *MLcore* is running in each `Task`

Compared to previous version, a variety of new algorithms were added on SONA, such as Deep & Cross Network (DCN) and 
Attention Factorization Machines (AFM). As can be seen from Figure 2, there are significant differences 
between algorithms on SONA and those on Spark: algorithms on SONA are mainly designated for recommendations 
and graph embedding, while algorithms on Spark tend to be more general-purpose. 

| ![sona_fig02](docs/imgs/sona_fig02.png) |
|  :----:    |
| *Figure 3: Algorithms comparison of Spark and Angel*   |

As a result, SONA can serve as a supplement of Spark

| ![sparkonangel](docs/imgs/sparkonangel.gif) |
|  :----:    |
| *Figure 4: Programming Example of SONA*   |


Figure 4 provides an example of running distributed machine learning algorithms on SONA, including following steps:
- Start parameter server at the beginning and stop it in the end.
- Load training and test data as Spark DataFrame.
- Define an Angel model and set parameters in Spark fashion. In this example, the algorithm is defined as a computing graph via JSON.
- Use “fit” method to train the model. 
- Use “evaluate” method to evaluate the trained model. 


## Quick Start
SONA supports three types of runtime models: YARN, K8s and Local. The local mode enable it easy to debug. 

The SONA job is essentially a Spark Application with an associated Angel-PS application. 
After the job is successfully submitted, there will be two separate Applications on the cluster, 
one is the Spark Application and the other is the Angel-PS Application. The two Applications are not coupled. 
If the SONA job is deleted, users are required to kill both the Spark and Angel-PS Applications manually.

```bash
#! /bin/bash
- cd angel-<version>-bin/bin; 
- ./SONA-example
```

The context of the submit scripts is as following:
```bash
#! /bin/bash
source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --queue g_teg_angel-offline \
    --jars $SONA_SPARK_JARS \
    --name "BreezeSGD-spark-on-angel" \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class com.tencent.angel.spark.examples.ml.BreezeSGD \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar
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
  .master("local[2]")
  .appName("AngelClassification")
  .getOrCreate()

val libsvm = spark.read.format("libsvmex")
val dummy = spark.read.format("dummy")

val trainData = libsvm.load("./data/angel/census/census_148d_train.libsvm")

val classifier = new AngelClassifier()
  .setModelJsonFile("./angelml/src/test/jsons/daw.json")
  .setNumClass(2)
  .setNumBatch(10)
  .setMaxIter(2)
  .setLearningRate(0.1)
  .setNumField(13)

val model = classifier.fit(trainData)

model.write.overwrite().save("trained_models/daw")
```

 
## Algorithms
- machine learning algorithms:
    + Traditional Machine Learning Methods
        - [Logistic Regression(LR)](docs/algo/lr_sona_en.md)
        - [Support Vector Machine(SVM)](docs/algo/svm_sona_en.md)
        - [Factorization Machine(FM)](docs/algo/fm_sona_en.md)
        - [Linear Regression](docs/algo/linreg_sona_en.md)
        - [Robust Regression](docs/algo/robust_sona_en.md)
        - [Gradient Boosting Decision Tree](docs/GBDT.md)
        - [Hyper-Parameter Tuning](docs/AutoML.md)
    + Deep Learning Methods
        - [Deep Neural Network(DNN)](docs/algo/dnn_sona_en.md)
        - [Mix Logistic Regression(MLR)](docs/algo/mlr_sona_en.md)
        - [Deep And Wide(DAW)](docs/algo/daw_sona_en.md)
        - [Deep Factorization Machine(DeepFM)](docs/algo/deepfm_sona_en.md)
        - [Neural Factorization Machine(NFM)](docs/algo/nfm_sona_en.md)
        - [Product Neural Network(PNN)](docs/algo/pnn_sona_en.md)
        - [Attention Factorization Machine(AFM)](docs/algo/afm_sona_en.md)
        - [Deep Cross Network(DCN)](docs/algo/dcn_sona_en.md)
- graph algorithms:
    + [Word2Vec](docs/algo/word2vec_sona_en.md)
    + [LINE](docs/algo/line_sona_en.md)
    + [KCore](docs/algo/kcore_sona_en.md)
    + [Louvain](docs/algo/louvain_sona_en.md)

## Deployment

## Support
- QQ account: 20171688

## References

## Other Resources

