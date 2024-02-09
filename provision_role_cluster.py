import configparser
import time
from botocore.exceptions import ClientError
import boto3
import json


def create_role(DWH_IAM_ROLE_NAME, KEY, SECRET, REGION):
    """
    Using information in config file, creates IAM role and attaches policy
    while writing to config file for future use.

    Args:
        DWH_IAM_ROLE_NAME: role name for parameter to be set to 
        KEY: AWS KEY
        SECRET: AWS SECRET
        REGION: AWS REGION

    Returns:
        none
    """
    
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=REGION)

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

    print(f'Role created: {roleArn}')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config.set('IAM', 'DWH_ROLE_ARN', roleArn)


def create_cluster(DWH_CLUSTER_IDENTIFIER, KEY, SECRET, REGION):
    """
    Using information in config file, creates cluster
    while writing endpoint to config file for future use.

    Args:
        DWH_CLUSTER_IDENTIFIER: Cluster Identifier 
        KEY: AWS KEY
        SECRET: AWS SECRET
        REGION: AWS REGION

    Returns:
        none
    """
    #Access Config File
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
    
    DWH_DB_USER = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DB_PORT")
    DWH_DB = config.get("CLUSTER","DB_NAME")
    
    roleArn = config.get("IAM","DWH_ROLE_ARN")
    
    redshift = boto3.client('redshift',
                       region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    
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
   
    # Wait for cluster
    print('Redshift Cluster is getting created...')
    cluster_status = 'creating'
    while cluster_status != "available":
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = myClusterProps['ClusterStatus']
        time.sleep(5)
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    print(f'Cluster status: {cluster_status} \nEndpoint: {DWH_ENDPOINT}')

    #1.5 Retrieve Cluster information
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    #1.6 Add endpoint to Config
    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)