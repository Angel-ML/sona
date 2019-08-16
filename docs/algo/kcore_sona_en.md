# K-CORE

> The K-CORE (k-degenerate-graph) algorithm is an important index in complex network research with 
a wide range of applications.

## 1. Algorithm Introduction
We implemented k-core algorithm for large-scale networks based on Spark On Angel.
The ps maintains the node's latest estimation of coreness along with the version number it was last updated for each node.
The Spark side maintains the adjacency list of the network, and pulls the latest coreness estimate in each round.
According to the h-index definition of the network node, the coreness estimate of the node is updated, and then pushed back to the ps in each round.
The algorithm stops until none of corenesses of nodes are updated last round.

## 2. Running

### Parameters

- input： hdfs path of input data, one line for each edge, separate by blank or a comma
- output： hdfs path of output, each line for a pair of node id with its coreness, separated by tap
- partitionNum： the number of partition
- enableReIndex： reindex the node id continuously and start at 0
- switchRate： rate to switch the calculation patten, default to 0.001. 

### Submitting scripts

Several steps must be done before editing the submitting script and running.

1. confirm Hadoop and Spark have ready in your environment
2. unzip angel-<version>-bin.zip to local directory (ANGEL_HOME)
3. upload angel-<version>-bin directory to HDFS (ANGEL_HDFS_HOME)
4. Edit $ANGEL_HOME/bin/spark-on-angel-env.sh, set SPARK_HOME, ANGEL_HOME, ANGEL_HDFS_HOME and ANGEL_VERSION

Here's an example of submitting scripts, remember to adjust the parameters and fill in the paths according to your own task.

```
HADOOP_HOME=my-hadoop-home
input=hdfs://my-hdfs/data
output=hdfs:hdfs://my-hdfs/model
queue=my-queue

export HADOOP_HOME=$HADOOP_HOME
source ./bin/spark-on-angel-env.sh 
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.yarn.allocation.am.maxMemory=55g \
  --conf spark.yarn.allocation.executor.maxMemory=55g \
  --conf spark.driver.maxResultSize=20g \
  --conf spark.kryoserializer.buffer.max=2000m\
  --conf spark.ps.instances=20 \
  --conf spark.ps.cores=4 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=15g \
  --conf spark.ps.log.level=INFO \
  --conf spark.offline.evaluate=200 \
  --conf spark.hadoop.angel.model.partitioner.max.partition.number=1000\
  --conf spark.hadoop.angel.ps.backup.interval.ms=2000000000 \
  --conf spark.hadoop.angel.matrixtransfer.request.timeout.ms=60000\
  --conf spark.hadoop.angel.ps.jvm.direct.factor.use.direct.buff=0.20\
  --queue $queue \
  --name "kcore angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 15g \
  --num-executors 50 \
  --executor-cores 3 \
  --executor-memory 15g \
  --class org.apache.spark.angel.examples.graph.KCoreExample \
  ./lib/angelml-$SONA_VERSION.jar \
  input:$input output:$output partitionNum:10 sep:space psPartitionNum:40 useBalancePartition:true storageLevel:MEMORY_ONLY
  ```