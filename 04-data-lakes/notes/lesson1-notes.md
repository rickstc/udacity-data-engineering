# The Power of Spark

## Introduction

Spark is one of the most popular tools for big data analytics. Spark is generally faster than Hadoop.
Lesson Outline:

- What is big data?
- Review of the hardware behind big data
- Introduction to distributed systems
- Brief history of Spark and big data
- Common Spark Use Cases
- Other Technologies in the big data ecosystem

## What is Big Data

No single definition of big data, however, big data often:

- Requires multiple, distributed computers

## Numbers Everyone Should Know

- CPU (fastest)
- Memory (2nd)
- Storage (3rd)
- Network (slowest)

## Hardware: CPU

- Runs mathmatical calculations
- Stores small amounts of data inside registers

## Hardware: Memory

- RAM Limitations
  - Cost
  - Temporary Data Storage
- RAM is about 250x slower than CPU

## Hardware: Storage

- Storage is much slower than RAM, however, it is much cheaper

## Hardware: Network

- Shuffling - Moving data between computers

Spark is designed to minimize shuffling

## Hardware: Key Ratios

- CPU - 200x faster than memory
- Memory - 15x Faster than SSD
- SSD - 20x faster than network
- Network (Slowest)

## Small Data Numbers

Data could be loaded into RAM on a single machine
Complex distributed systems wouldn't be worth it

## Big Data Numbers

Exceeds the processing capacity of a single machine, for example

## Medium Data Numbers

When processing can be split into chunks to circumvent RAM / CPU limitations

## History of Distributed Computing

Distributed computing assumes that each CPU has it's own dedicated memory and is connected to other nodes across a network
Parallel computing implies that multiple CPUs share the same memory

## The Hadoop Ecosystem

### Vocabulary

- Hadoop - An ecosystem of tools for big data storage and data analysis
- Hadoop MapReduce - A system for processing and analyzing large data sets in parallel
- Hadoop YARN - A resource manager that schedules jobs across a cluster
- Hadoop Distributed File System (HDFS) - A big data storage system that splits data into chunks and stores the chunks across a cluster of computers
- Apache Pig - A SQL-like language that runs on top of Hadoop MapReduce
- Apache Hive - Another SQL-like interface that runs on top of Hadoop MapReduce

### How is Spark related to Hadoop?

Spark is another big data framework containing libraries for data analysis, machine learning, graph analysis, and streaming live data
Spark is generally faster than Hadoop because it does not write intermediate results to disk, instead trying to keep intermediate results in memory where possible
Spark can leverage HDFS, other sources, or S3

### Streaming Data

Data streaming is used when you want to store and analyze data in real-time
Spark has a streaming library called Spark Streaming, though it is not as popular as rivals such as Storm and Flink

## MapReduce

Programming technique for manipulating large data sets. "Hadoop MapReduce" is a specific implementation of this programming technique.

MapReduce divides up a large dataset and distributes the data across a cluster. In the map step, each data is analyzed and converted into a (key, value) pair. The key-value paris are shuffled across the cluster so that all keys are on the same machine. In the reduce step, the values with the same keys are combined together.

Spark does not implement map reduce, however spark programs can be written to behave similarly to the map-reduce paradigm.

## Hadoop MapReduce [ Demo ]

## The Spark Cluster

Modes:

- Local Mode - Everything on one machine
- Distributed

## Spark Use Cases

ETL:

- Extract
- Transform
- Load

Logistic Regression
Page Rank

### You Don't Always Need Spark

Spark is meant for big data sets that cannot fit on one computer. For single-node use cases:

- AWK - Command line tool for manipulating text files
- R - A programming language and software environment for statistical computing
- Python PyData Stack - pandas, Matplotlib, MumPy, scikit-learn

Scikit-learn has a range of algorithms for machine learning, including those for classification, regression, clustering and preprocessing
More complex algorithms like deep learning may require TensorFlow or PyTorch

### Spark's Limitations

Spark Streaming's latency is at least 500 ms as opposed to native streaming tools such as Storm, Apex or Flink
Spark only supports linear scale machine learning algorithms, and excludes deep learning

## Summary
