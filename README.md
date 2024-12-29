# WIH3003_UM

## Introduction

Hadoop and Spark are both distributed computing frameworks designed to process and analyze large-scale data. [Hadoop MapReduce](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) is a Java-based model for distributed computing, with key tasks including Map (transforming data into key/value pairs) and Reduce (shrinking data tuples). The processing is done at slave nodes, and the result is sent to the master node. 

In contrast, [Apache Spark](https://spark.apache.org/) is a fast cluster computing tool that builds on Hadoop MapReduce, emphasizing in-memory cluster computing for speed. It uses Resilient Distributed Datasets (RDDs) as its basic data structure, ensuring fault tolerance and immutability. RDDs can include various objects from Java or Scala for data processing.

## Methodology

We aim to compare the performance of Hadoop and Spark frameworks in processing a ecommerce CSV file that has been loaded into the Hadoop Distributed File System (HDFS). Three `java` programs are compiled into `jar` files and executed through the command line interface, and three `scala` scripts are being runned in the scala CLI. 

Those built jobs will process the input file and store the output in another file. The queries we perform on both frameworks are:

1. Determining the total sales of each product category.
2. Analyzing order count by city.
3. Conducting a word count on the dataset. 

To evaluate the efficiency, we employ metrics such as execution time, throughput, and memory consumption.

$$Throughput = Number\_of\_Records / Execution\_Time$$

This process is **repeated five times**, and the **average values** of the metrics are calculated for a comprehensive assessment of Hadoop’s performance.

---

## Prerequisite

For this project, you will need:

1. Linux Terminal or Virtual Machine on Cloud Platform with Hadoop and Spark installed
    
    We used [Oracle VirtualBox](https://www.oracle.com/virtualization/technologies/vm/downloads/virtualbox-downloads.html) to access our [Cloudera VM](https://www.cloudera.com/downloads/cdp-private-cloud-trial.html).

2. Suitable Dataset
    
    We used the [E-Commerce Order Dataset](https://www.kaggle.com/datasets/bytadit/ecommerce-order-dataset) from Kaggle to perform performance analysis.

## Setup

Download the `java` and `scala` files from this repository

```sh
git clone https://github.com/LimJY03/WIH3003_UM.git
cd WIH3003_UM
```

## Execute

1. Store the dataset in Hadoop Distributed File System (HDFS).
2. For Hadoop, compile the `java` file in the respective folder into a `jar` file, then execute it on the dataset.
3. For Spark, go into the `spark` CLI, copy the `spark` file in the respective folder and execute the commands line by line.
4. Repeat Step 2 and Step 3 each for 5 times, note down the performance metrics after every execution for comparison analysis.