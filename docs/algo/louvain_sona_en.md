# Louvain(FastUnfolding)

The Louvain (FastUnfolding) algorithm is a classic community detection algorithm, optimized by [modularity] (https://en.wikipedia.org/wiki/Modularity_(networks):
$$Q = \frac{1}{2m} \sum_{vw}\left[A_{vw} - \frac{k_vk_w}{2m}\right]\delta(c_v, c_w).$$

## 1. Introduction to the algorithm
Louvain algorithm contains two processes
 - Modular optimization
 - Community folding
We maintain the community id of the node and the weight information corresponding to the community id through two ps vectors. Each worker on the Spark side maintains a part of the node and the corresponding adjacency information, including the neighbors of the node and the corresponding edge weights.
- In the module optimization phase, each worker calculates the new community attribution of its own maintenance node based on the degree of module change. The community attribution update is updated to ps in real time in the form of a batch.
- In the community folding phase, we construct a new network based on the current community ownership, where the new network node corresponds to the community of the pre-folding network, and the new edge corresponds to the sum of the weights of the direct nodes of the pre-folding network community. . Before starting the next stage of modularity optimization, we need to correct the community id so that the id of each community is identified as the id of a node in the community. Here we use the smallest identifier for all node ids in the community.

## 2. Running

### Parameters

- input: hdfs path, network data, two long integer id nodes per line (if the weighted network, the third float represents the weight), separated by white space or comma, indicating an edge
- output: hdfs path, the community id of the output node, one data per line, indicating the community id value corresponding to the node, separated by tap
- numFold: number of folds
- numOpt: number of module optimizations per round
-eps: module degree increment limit
- batchSize: the size of the node update batch
- partitionNum: Enter the number of data partitions
- isWeighted: Is it entitled?

### Submitting scripts

Several steps must be done before editing the submitting script and running.

1. confirm Hadoop and Spark have ready in your environment
2. unzip sona-<version>-bin.zip to local directory (SONA_HOME)
3. upload sona-<version>-bin directory to HDFS (SONA_HDFS_HOME)
4. Edit $SONA_HOME/bin/spark-on-angel-env.sh, set SPARK_HOME, SONA_HOME, SONA_HDFS_HOME and ANGEL_VERSION

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
  --name "line angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 15g \
  --num-executors 50 \
  --executor-cores 3 \
  --executor-memory 15g \
  --class org.apache.spark.angel.examples.graph.LouvainExample \
  ./lib/angelml-$SONA_VERSION.jar \
  input:$input output:$output partitionNum:10 sep:space numFold:5 batchSize:10000  psPartitionNum:40 useBalancePartition:true storageLevel:MEMORY_ONLY
```