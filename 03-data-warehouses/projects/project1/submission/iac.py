"""
File: iac.py
Attribution: Student
Background:
This file contains the necessary code to setup and tear down a Redshift Cluster.
While not officially part of the project, the student wanted a testable way to
ensure a consistent environment while minimizing costs.
"""

import boto3
import os
import json
from botocore.config import Config
import sys
import datetime


def json_encode_dt(obj):
    if isinstance(obj, (datetime.datetime)):
        return obj.isoformat()


class AWSHelper:
    def __init__(self) -> None:
        self._load_credentials()

        self.redshift = boto3.client(
            'redshift',
            config=self.aws_config,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )

    def _load_credentials(self):
        """
        Loads AWS Credentials from ./credentials.json
        While not ordinarily necessary; the student has several
        different credentials on this machine for other projects.
        Therefore, by loading credentials from a file in this directory,
        the student can be explicit about which set of credentials and which account
        to use for this project.
        """
        credential_fp = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)
            ),
            'credentials.json'
        )
        if not os.path.exists(credential_fp):
            raise Exception(
                "A credentials.json in the root of this project is missing")

        with open(credential_fp, 'r') as credential_file:
            credentials = json.load(credential_file)

        self.aws_config = Config(
            region_name=credentials.get('region')
        )
        self.key = credentials.get("key")
        self.secret = credentials.get("secret")

        self.config_fp = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__)
            ),
            'config.json'
        )
        if not os.path.exists(self.config_fp):
            raise Exception(
                "A config.json in the root of this project is missing")

        with open(self.config_fp, 'r') as config_file:
            config = json.load(config_file)

        self.config = config.get('production')
        self.cluster = self.config.get('cluster')

    def create_cluster(self):
        """ Creates a Redshift Cluster """
        response = self.redshift.create_cluster(
            DBName=self.cluster.get('db_name'),
            ClusterIdentifier=self.cluster.get('cluster_identifier'),
            ClusterType='single-node',
            NodeType='dc2.large',
            MasterUsername=self.cluster.get('db_user'),
            MasterUserPassword=self.cluster.get('db_password'),
            VpcSecurityGroupIds=[
                self.cluster.get('sg_id')
            ],
            AvailabilityZone=self.cluster.get('az'),
            PubliclyAccessible=True,
            Encrypted=False,
            IamRoles=[
                self.cluster.get('iam_role')
            ]
        )
        print(json.dumps(response, indent=2, default=json_encode_dt))
        return response

    def delete_cluster(self):
        """ Removes the Redshift Cluster """
        response = self.redshift.delete_cluster(
            ClusterIdentifier=self.cluster.get('cluster_identifier'),
            SkipFinalClusterSnapshot=True
        )
        print(json.dumps(response, indent=2, default=json_encode_dt))
        return response

    def get_cluster_status(self):
        response = self.redshift.describe_clusters(
            ClusterIdentifier=self.cluster.get('cluster_identifier')
        )
        return response

    def write_endpoint_to_config(self, cluster_status=None):
        if cluster_status is None:
            cluster_status = self.get_cluster_status()
        if cluster_status['Clusters'][0]['ClusterStatus'] == 'available':
            with open(self.config_fp, 'r') as config_file:
                config = json.load(config_file)

            with open(self.config_fp, 'w') as config_file:
                config['production']['cluster']['host'] = cluster_status['Clusters'][0]['Endpoint']['Address']
                config_file.write(json.dumps(config, indent=2))
        else:
            print("Cluster is not available")


if __name__ == '__main__':
    """
    This relies on sys.argv to determine what action to take on the redshift cluster

    System Arguments:
    start - this will create the cluster
    stop - this will delete the cluster
    None - the default case, this will get the status of the cluster and write it to the configuration file

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
        aws.write_endpoint_to_config(status)
