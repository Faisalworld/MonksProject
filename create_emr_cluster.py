"""
It was giving problem, so I added new policy(AdministratorAccess) to MyLambdaEmrFunction-role-2wacafzy(this role was created automatically) this role.

"""

import boto3

client = boto3.client('emr')
def lambda_handler(event, context):
    response = client.run_job_flow(
        Name='MyEMRCluster',
        LogUri='s3://spark-faisal-spark/emr_logs',
        ReleaseLabel='emr-6.0.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'Ec2KeyName': 'spark',
            'InstanceCount': 2,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-0b839ae7776c2129d'
        },
        Applications=[{'Name': 'Spark'}],
        Configurations=[
            {'Classification': 'spark-hive-site',
             'Properties': {
                 'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
             }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Steps=[]
    )
    return response["JobFlowId"]