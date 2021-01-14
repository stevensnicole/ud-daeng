import pandas as pd
import boto3
import botocore
import json
import configparser
import logging

logging.basicConfig(level=logging.INFO)


#Read the config file and the secrets file (secrets ignored in .gitignore)
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

secret_config = configparser.ConfigParser()
secret_config.read_file(open('amzn-keys.cfg'))

#Set the config and secrets file values into variables
KEY                    = secret_config.get('SECRETS','AMZN_KEY')
SECRET                 = secret_config.get('SECRETS','AMZN_SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
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

#Create an IAM role which will allow SQL calls to read from S3 buckets
try:
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)

#Attach the role to the policy
iam.attach_role_policy(
    PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
    RoleName=DWH_IAM_ROLE_NAME
)


roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)

#Create the redshift culster
try:
    response = redshift.create_cluster( 
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            Port=int(DWH_PORT),
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,        
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            IamRoles=[roleArn['Role']['Arn']]
    )
except Exception as e:
    print(e)

#Use a waiter to wait for the cluster to come up
waiter = redshift.get_waiter('cluster_available')

try:
	logging.info('Waiting for cluster to be available')
	waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

except botocore.exceptions.WaiterError as wex:
    logging.error('The cluster didnt become available. {}'.format(wex))


#Once the cluster is up, output the endpoint and ARN
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
logging.info('Endpoint - %s', myClusterProps['Endpoint']['Address'])
logging.info('Role ARN - %s', myClusterProps['IamRoles'][0]['IamRoleArn'])

#Set the created host and arn in the configuration file
config.set("CLUSTER", "HOST", myClusterProps['Endpoint']['Address'])
config.set("IAM_ROLE", "ARN", myClusterProps['IamRoles'][0]['IamRoleArn'])
with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)
