# Implementing Data Warehouses on AWS

## Lesson Introduction

DWH on AWS Objectives:

- DWH on AWS, Why?
- Amazon Redshift Architecture
- General ETL Concepts
- ETL for Redshift
- Building a Redshift Cluster: Part 1 - Logistics
- Building a Redshift Cluster: Part 2 - Infrastructure as Code
- Optimizing Redshift Table Design

## Choices for Implementing a Data Warehouse

On-Premise

- Heterogeneity, scalability, elasticity of the tools, technologies and processes
- Need for diverse IT staff skills & multiple locations
- Cost of Ownership

Cloud

- Lower barrier to entry
- May add as you need and change
- Scalability & elasticity out of the box

Operational cost might be high and heterogeneity/complexity won't disappear

# DWY Dimensional Model Storage on AWS

Cloud-Managed

- Amazon RDS
- Amazon DynamoDB
- Amazon S3

vs.

Self Managed

- EC2 /w Postgres
- EC2 /w Cassandra
- EC2 /w Unix FS

## Amazon Redshift Technology

Redshift

- Column-oriented storage
- Best suited for storing OLAP workloads, summing over a long history
- Internally, it's a modified postgresql

Traditional Relational

- Most relational databases execute multiple queries in parallel if they have access to many cores/servers
- However, every query is always executed on a single CPU of a single machine
- Acceptable for OLTP, mostly updates and few row retrieval

Redshift

- Massively Parallel Processing (MPP) database parallelize the execution of one query on multiple CPUs/machines
- How? A table is partitioned and partitions are processed in parallel
- Amazon Redshift is a cloud-managed, column-oriented, MPP database
- Other examples include Teradata Aster, Oracle ExaData and Azure SQL

## Amazon Redshift Architecture

Redshift Cluster:

- 1 Leader node
- 1 or more compute nodes

Leader Node:

- Coordinates compute nodes
- Handles external communication
- Optimizes query execution

Compute Nodes:

- Each with own CPU, memory, and disk (determined by node type)
- Scale up: get more powerful nodes
- Scale out: get more nodes

Node Slices:

- Each compute node is logically dived into a number of slices
- A cluster with n slices, can process n partitions of tables simultaneously

## SQL to SQL ETL

ETL Server runs SELECT queries on source DB server, stores them in CSV files, then inserts or copies data over to the Destination DB server

## SQL to SQL ETL - AWS Case

RDS to S3 to Redshift

## Redshift ETL in Context

Source Info to S3 to Redshift to S3 to BI Apps

## Ingesting at Scale: Use Copy

- To transfer data from an S3 staging area to redshift use the COPY command
- Inserting data row by using INSERT will be very slow
- If the file is large:
  - It is better to bak it up to multiple files
  - Ingest in Parallel
    - Either using a common prefix
    - Or a manifest file
- Other considerations:
  - Better to ingest from the same AWS region
  - Better to compress all the CSV files
- One can also specify the delimiter to be used

## Redshift ETL Continued

- The optimal compression strategy for each column type is different
- Redshift gives the user control over the compression of each column
- The COPY command makes automatic best-effort compression decisions for each column

ETL from Other Sources

- It is also possible to ingest directly using SSH from EC2 machines
- Other than that:
  - S3 needs to be used as a staging area
  - Usually, an EC2 ETL worker needs to run the ingestion jobs orchestrated by a dataflow product like Airflow, Luigi, Nifi, StreamSet or AWS Data Pipeline

ETL out of Redshift

- Redshift is accessible, like any relational database, as a JDBC/ODBC source
  - Naturally used by BI apps
- However, we may need to extract data out of Redshift to pre-aggregated OLAP cubes

## Optimizing Table Design

- When a table is partitioned and distributed across slices in different machines, this is done blindly
- If we have insight into frequent access patterns, we can choose from one of two strategies:
  - Distributing Style
  - Sorting Key

## Distribution Style: Even

- Round-Robin over all slices to achieve load-balancing
- Good if a table won't be joined

Joins on tables distributed evenly are very inefficient

## Distribution Style: All

- Small tables could be replicated on all slices to speed up joins
- Used frequently for dimension tables
- Also known as "Broadcasting"

Fact tables could be distributed EVENly, while dimension tables could be replicated, for example

## Distribution Style: Auto

- Leave decision to Redshift
- "Small Enough" tables are distributed with an ALL strategy
- Large tables are distributed with EVEN strategy

## Distribution Style: Key

- Rows having similar values are placed in the same slice

For example, facts distributed by particular dimension grouped into the same slice

- This can lead to a skewed distribution if some values of the dist key are more frequent than others
- However, very useful when a dimension table is too big to be distributed with ALL strategy. In that case, we distribute both the fact table and dimension table using the same dist key
- If two tables are distributed on the joining keys, redshift collocates the rows from both tables on the same slices

## Sorting Key

- One can define its columns as a sort key
- Upon loading, rows are sorted before distribution to slices
- Minimizes the query time since each node already has contiguous ranges of rows based on the sorting key
- Useful for columns that are used frequently in sorting like the date dimension and its corresponding foreign key in the fact table
