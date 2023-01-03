# Setting Up Spark CLusters with AWS

## Introduction
## From Local to Standalone Mode
Spark Clusters are set up in EC2
Data will be stored in Amazon S3
## Setup Instructions AWS
## AWS - Install and Configure CLI v2
Install the CLI
Create IAM User
Configure the CLI

## AWS CLI - Create EMR Cluster

`aws emr create-cluster` - command to create a cluster


    Provide cluster name, EC2 private key file name, and profile name
    aws emr create-cluster \
    --name <YOUR_CLUSTER_NAME> \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName=<Key-pair-file-name>, SubnetId=<subnet-Id> \
    --instance-type m5.xlarge \
    --auto-terminate \
    --profile <profile-name>



## Using Notebooks on Your Cluster
## Spark Scripts
Python scripts are much more scalable.
## Submitting Spark Scripts
## Storing and Retrieving Data on the Cloud
## Reading and Writing to Amazon S3
## Understanding difference between HDFS and AWS S3
## Reading and Writing Data to HDFS
## Recap Local Mode to Cluster Mode