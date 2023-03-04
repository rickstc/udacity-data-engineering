"""
File: iac.py
Attribution: Student
Background:
This file contains the necessary code to setup and tear down a Redshift Cluster.
While not officially part of the project, the student wanted a testable way to
ensure a consistent environment while minimizing costs.
"""
import configparser

import boto3
import os
import json
from botocore.config import Config
import sys
import datetime
from botocore.errorfactory import ClientError


def json_encode_dt(obj):
    if isinstance(obj, (datetime.datetime)):
        return obj.isoformat()


class AWSHelper:
    def __init__(self) -> None:
        self._load_credentials()

        self.redshift = boto3.client(
            'redshift',
            region_name=self.config['CLUSTER']['REGION']
        )

    def _load_credentials(self):
        """
        Reads configuration from the config file and sets the following properties
        on the instance
        - self.aws_config
        - self.config
        - self.cluster - shortcut to self.config['CLUSTER]
        """

        self.config_fp = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)
            ),
            'dwh.cfg'
        )
        if not os.path.exists(self.config_fp):
            raise Exception(
                "A dwh.cfg in the root of this project is missing")

        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        self.config = config
        self.cluster = config['CLUSTER']

    def create_cluster(self):
        """ Creates a Redshift Cluster """
        response = self.redshift.create_cluster(
            DBName=self.cluster['DB_NAME'],
            ClusterIdentifier=self.cluster['IDENTIFIER'],
            ClusterType='single-node',
            NodeType='dc2.large',
            MasterUsername=self.cluster['DB_USER'],
            MasterUserPassword=self.cluster['DB_PASSWORD'],
            VpcSecurityGroupIds=[
                self.cluster['SG_ID']
            ],
            PubliclyAccessible=True,
            Encrypted=False,
            IamRoles=[
                self.config['IAM_ROLE']['ARN']
            ]
        )
        print(json.dumps(response, indent=2, default=json_encode_dt))
        return response

    def delete_cluster(self):
        """ Removes the Redshift Cluster """
        response = self.redshift.delete_cluster(
            ClusterIdentifier=self.cluster['IDENTIFIER'],
            SkipFinalClusterSnapshot=True
        )
        print(json.dumps(response, indent=2, default=json_encode_dt))
        return response

    def get_cluster_status(self):
        response = self.redshift.describe_clusters(
            ClusterIdentifier=self.cluster['IDENTIFIER']
        )
        return response


if __name__ == '__main__':
    """
    This relies on sys.argv to determine what action to take on the redshift cluster

    System Arguments:
    start - this will create the cluster
    stop - this will delete the cluster
    None - the default case, this will get the status of the cluster

    Example Usage:
    - python iac.py start
    - python iac.py stop
    - python iac.py
    """
    aws = AWSHelper()

    if 'start' in sys.argv:
        aws.create_cluster()

    if 'stop' in sys.argv:
        aws.delete_cluster()

    else:
        status = aws.get_cluster_status()
        print(
            f"The cluster's status is: {status['Clusters'][0]['ClusterStatus']}")
