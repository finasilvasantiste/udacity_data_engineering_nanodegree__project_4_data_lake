from aws.aws_entities.AWSClient import AWSClient
import configparser

config = configparser.ConfigParser()
config.read_file((open(r'dl.cfg')))


class EMRCluster:
    """ Represents an AWS EMR Cluster. """

    def __init__(self):
        self.emr_client = None
        self.subnet_id = config.get('EMR_CLUSTER', 'SUBNET_NET_ID')
        self.key_name = config.get('EMR_CLUSTER', 'KEY_NAME')

    def create_resources(self):
        self.emr_client = AWSClient(client_name='emr').client

        cluster_id = self.emr_client.run_job_flow(
            Name="Boto3 test cluster",
            ReleaseLabel='emr-5.12.0',
            Instances={
                'MasterInstanceType': 'm4.xlarge',
                'SlaveInstanceType': 'm4.xlarge',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': self.subnet_id,
                'Ec2KeyName': self.key_name,
            },
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        print('cluster created with the step...', cluster_id['JobFlowId'])
