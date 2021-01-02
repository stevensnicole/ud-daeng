import pandas as pd
import boto3
import botocore
import json
import configparser
import logging

logging.basicConfig(level=logging.INFO)

#Read the config file and the secrets file (secrets ignored in .gitignore)
secret_config = configparser.ConfigParser()
secret_config.read_file(open('amzn-keys.cfg'))

KEY                    = secret_config.get('SECRETS','AMZN_KEY')
SECRET                 = secret_config.get('SECRETS','AMZN_SECRET')

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_IDENTIFIER", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME]
             })

#Create a redshift client
redshift = boto3.client('redshift',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

#Create iam client
iam = boto3.client('iam',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

#Delete the cluster
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

#Use a waiter to wait for the cluster to be deleted
waiter = redshift.get_waiter('cluster_deleted')

try:
	logging.info('Waiting for cluster to be deleted')
	waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

except botocore.exceptions.WaiterError as wex:
    logging.error('The cluster didnt delete. {}'.format(wex))

#Remove the Policy
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

#Blank out the host and arn in the configuration file
config.set("CLUSTER", "HOST", "")
config.set("IAM_ROLE", "ARN", "")
with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)

