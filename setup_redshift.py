"""Sets up a redshift cluster if specified in the configuration does not exist"""
import boto3
from configparser import ConfigParser
from aws_credentials import get_aws_credentials
from logger_cfg import setup_logger
import json

# Set up logging
logger = setup_logger(__file__) # name of the logger

# Read configuration file and get aws credentials
dwh_cfg = ConfigParser()
dwh_cfg.read('dwh.cfg')
AWS_PROFILE, AWS_REGION = dwh_cfg['AWS']['PROFILE'], dwh_cfg['AWS']['REGION']
AWS_KEY, AWS_SECRET = get_aws_credentials(AWS_PROFILE)

# Set up clients
session = boto3.Session(
    region_name=AWS_REGION, 
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)

ec2 = session.resource('ec2')
s3 = session.resource('s3')
iam = session.client('iam')
redshift = session.client('redshift')

# Create Redshift role (if it doesn't exist - otherwise skip)
logger.info('Creating IAM Role')
policy = {
    'Version': '2012-10-17',
    'Statement':[{
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {"Service": "redshift.amazonaws.com"}
    }]
}

try: # try creating role for the first time
    redshift_role = iam.create_role(
        Path='/',
        RoleName=dwh_cfg['CLUSTER']['IAM_ROLE_NAME'],
        AssumeRolePolicyDocument=json.dumps(policy)
    )
    logger.info('Creating IAM Role - success!')
except Exception as e: 
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        logger.debug(e.response['Error']['Message'])
        logger.info('Creating IAM Role - skipped!')
    else:
        logger.error(e)

# Provide S3 Access to cluster
logger.info('Attaching Policy')
iam.attach_role_policy(
    RoleName=dwh_cfg['CLUSTER']['IAM_ROLE_NAME'],
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)  
logger.info('Attaching Policy - success!')

# Writing the role ARN to config file for ETL scripts
logger.info('Writing role ARN to config file')
ROLE_ARN = iam.get_role(RoleName=dwh_cfg['CLUSTER']['IAM_ROLE_NAME'])['Role']['Arn']
logger.info(f"The Role ARN is {ROLE_ARN}")

dwh_cfg.set('IAM_ROLE', 'ARN', ROLE_ARN) # writing to file
with open('dwh.cfg', 'w') as configfile:
    dwh_cfg.write(configfile)
logger.info('Writing role ARN to config file - success!')

# Create cluster
logger.info('Creating Redshift Cluster')
try:
    redshift.create_cluster(        
        # Hardware params
        ClusterType=dwh_cfg['CLUSTER']['CLUSTER_TYPE'],
        NodeType=dwh_cfg['CLUSTER']['NODE_TYPE'],
        NumberOfNodes=int(dwh_cfg['CLUSTER']['NUM_NODES']),
        # Identiiers
        DBName=dwh_cfg['CLUSTER']['DB_NAME'],
        ClusterIdentifier=dwh_cfg['CLUSTER']['CLUSTER_IDENTIFIER'],
        MasterUsername=dwh_cfg['CLUSTER']['DB_USER'],
        MasterUserPassword=dwh_cfg['CLUSTER']['DB_PASSWORD'],
        IamRoles=[dwh_cfg['IAM_ROLE']['ARN']]
    )

    logger.info('Creating Redshift Cluster - success!')
except Exception as e:
    if e.response['Error']['Code'] == 'ClusterAlreadyExists':
        logger.debug(e.response['Error']['Message'])
        logger.info('Creating Redshift Cluster - skipped!')
    else: 
        logger.error(e)
    
logger.info('Waiting for cluster to be available...')
redshift_available_waiter = redshift.get_waiter('cluster_available')
redshift_available_waiter.wait(ClusterIdentifier=dwh_cfg['CLUSTER']['CLUSTER_IDENTIFIER'])

# write cluster endpoint to config file
cluster_desc = redshift.describe_clusters(ClusterIdentifier=dwh_cfg['CLUSTER']['CLUSTER_IDENTIFIER'])['Clusters'][0]
END_POINT = cluster_desc['Endpoint']['Address']
logger.info(f"The Cluster endpoint is {END_POINT}")

dwh_cfg.set('CLUSTER', 'HOST', END_POINT) # writing back to file
with open('dwh.cfg', 'w') as configfile:
    dwh_cfg.write(configfile)
logger.info('Cluster host endpoint saved to config file.')

# Add cluster to default security group and authorize access 
vpc = ec2.Vpc(id=cluster_desc['VpcId'])
default_sg = list(vpc.security_groups.all())[0]

logger.info('Authorizing ingress to cluster...')
try:
    default_sg.authorize_ingress(
            GroupName= default_sg.group_name, 
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_cfg['CLUSTER']['PORT']),
            ToPort=int(dwh_cfg['CLUSTER']['PORT'])
    )
except Exception as e:
    if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
        logger.debug(e.response['Error']['Message'])
        logger.info('Authorizing ingress to cluster - skipped!')
    else:
        logger.error(e)
else:
    logger.info('Authorizing ingress to cluster - success!')

# TODO: add snapshot and pause cluster to save costs








