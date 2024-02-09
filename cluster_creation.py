from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from botocore.exceptions import ClientError
import boto3
import json

#Import config
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

#Fetch keys
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

#Establish connection
iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_cluster(DWH_IAM_ROLE_NAME, DWH_CLUSTER_IDENTIFIER):
    """
    Using information in config file, creates required clusters and roles
    and write that information back to the config file for future use.
    """

    #1.1 Create the role 
    try:
        print("1.1 Creating a new IAM Role") 
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

    #1.2 Attach policy
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    #1.3 Get IAM role ARN
    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)

    #1.4 Create Cluster
    DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
    
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")
    DWH_DB = config.get("DWH","DWH_DB")
    
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)
   
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    #1.5 Retrieve Cluster information
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    #1.6 Add to config file.
    config.add_section('CLUSTER')
    config.set('CLUSTER', 'DWH_ENDPOINT', DWH_ENDPOINT)
    config.set('CLUSTER', 'DWH_ROLE_ARN', DWH_ROLE_ARN)
    
def delete_cluster(DWH_IAM_ROLE_NAME, DWH_CLUSTER_IDENTIFIER):
    """
    Using information in config file, deletes clusters and roles.
    """
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)