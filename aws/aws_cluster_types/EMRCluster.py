from aws.aws_entities.AWSClient import AWSClient
import configparser

config = configparser.ConfigParser()
config.read_file((open(r'dl.cfg')))


class EMRCluster:
    """ Represents an AWS EMR Cluster. """

    def __init__(self):
        self.subnet_id = config.get('EMR_CLUSTER', 'SUBNET_NET_ID')
        self.key_name = config.get('EMR_CLUSTER', 'KEY_NAME')
        self.job_flow_id = None
        self.emr_client = AWSClient(client_name='emr').client

    def create_resources(self):
        cluster_id = self.emr_client.run_job_flow(
            Name="Boto3 test cluster",
            ReleaseLabel='emr-5.12.0',
            Instances={
                'MasterInstanceType': 'm4.xlarge',
                'SlaveInstanceType': 'm4.xlarge',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': self.subnet_id
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        self.job_flow_id = cluster_id['JobFlowId']
        print('cluster created with the step...', cluster_id['JobFlowId'])

    def delete_resources(self):
        self.emr_client.terminate_job_flows(JobFlowIds=self.job_flow_id)
