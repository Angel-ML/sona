# SONA Overview
Spark On Angel (SONA), arming Spark with a powerful Parameter Server, which enable Spark to train very big models

Similar to Spark MLlib, Spark on Angel is a standalone machine learning library built on Spark (yet it does not rely on Spark MLlib). SONA was based on RDD APIs and only included model training step in previous versions. In Angel 3.0, we introduce various new features to SONA:
- Integration of feature engineering into SONA. Instead of simply borrowing Spark’s feature engineering operators, we add support for long index vector to all the operators to enable training of high dimensional sparse models. 
- Seamless connection with automatic hyperparameter tuning.
- Spark-fashion APIs that introduce no cost for Spark users to switch to Angel.
- Support for two new data formats: LibFFM and Dummy.

| ![sona_fig01](docs/imgs/sona_fig01.png) |
|  :----:    |
| *Figure 1: Architecture of SONA*   |

We also develop a variety of new algorithms on SONA, such as Deep & Cross Network (DCN) and 
Attention Factorization Machines (AFM). As can be seen from Figure 2, there are significant differences 
between algorithms on SONA and those on Spark: algorithms on SONA are mainly designated for recommendations 
and graph embedding, while algorithms on Spark tend to be more general-purpose. 

| ![sona_fig02](docs/imgs/sona_fig02.png) |
|  :----:    |
| *Figure 2: Algorithms comparison of Spark and Angel*   |

![](docs/imgs/sparkonangel.gif)

## Quick Start

## Algorithms
- machine learning algorithms:
    + Traditional Machine Learning Methods
        - [Logistic Regression(LR)](https://github.com/Angel-ML/angel/blob/master/docs/algo/angel_or_spark_on_angel.md)
        - [Support Vector Machine(SVM)](docs/SVM.md)
        - [Factorization Machine(FM)](docs/FM.md)
        - [Linear Regression](docs/LReg.md)
        - [Robust Regression](docs/RR.md)
    + Deep Learning Methods
        - [Deep Neural Network(DNN)](docs/DNN.md)
        - [Mix Logistic Regression(MLR)](docs/MLR.md)
        - [Deep And Wide(DAW)](docs/DAW.md)
        - [Deep Factorization Machine(DeepFM)](docs/DeepFM.md)
        - [Neural Factorization Machine(NFM)](docs/NFM.md)
        - [Product Neural Network(PNN)](docs/PNN.md)
        - [Attention Factorization Machine(AFM)](docs/AFM.md)
        - [Deep Cross Network(DCN)](docs/DCN.md)
- graph algorithms:
    + [Word2Vec](docs/Word2Vec.md)
    + [LINE](docs/LINE.md)
    + [KCore](docs/KCore.md)
    + [Louvain](docs/Word2Vec.md)

## Deployment

## Support
- QQ account: 20171688

## References

## Other Resources

