from aws.aws_entities.AWSClient import AWSClient
import configparser

config = configparser.ConfigParser()
config.read_file((open(r'dl.cfg')))


class EMRCluster:
    """ Represents an AWS EMR Cluster. """

    def __init__(self):
        self.subnet_id = config.get('EMR_CLUSTER', 'SUBNET_NET_ID')
        self.emr_client = AWSClient(client_name='emr').client
        self.key_name = config.get('EMR_CLUSTER', 'KEY_NAME')

    def create_resources(self):
        """
        Creates all EMR cluster resources needed.
        :return:
        """
        print('++++ CREATING EMR CLUSTER ++++')

        cluster_id = self.emr_client.run_job_flow(
            Name="Boto3 Project 4 Cluster",
            ReleaseLabel='emr-5.30.1',
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
            ServiceRole='EMR_DefaultRole',
            Applications=[
                {
                    'Name': 'Spark'
                },
                {
                    'Name': 'Zeppelin'
                },
                {
                    'Name': 'Hive'
                }
            ],
        )

        print('++++ SPINNING UP EMR CLUSTER WITH ID {} ++++'.format(cluster_id['JobFlowId']))

    def get_non_terminated_clusters(self):
        """
        Returns list with clusters that are not terminated.
        :return: list with cluster ids
        """
        cluster_ids = []
        page_iterator = self.emr_client.get_paginator('list_clusters').paginate(
            ClusterStates=['RUNNING', 'WAITING', 'STARTING']
        )
        print('++++ LIST OF CLUSTERS IN STATE: RUNNING, WAITING OR STARTING: ++++')
        for page in page_iterator:
            for item in page['Clusters']:
                cluster_id = item['Id']
                status = item['Status']['State']
                print('{} with state {}'.format(cluster_id, status))

                if status != 'TERMINATED':
                    cluster_ids.append(cluster_id)

        return cluster_ids

    def delete_resources(self):
        """
        Deletes all EMR cluster resources.
        :return:
        """
        cluster_ids = self.get_non_terminated_clusters()

        if len(cluster_ids) > 0:
            print('++++ DELETING CLUSTERS ++++')
            self.emr_client.terminate_job_flows(JobFlowIds=cluster_ids)
        else:
            print('++++ THERE ARE NO CLUSTERS NO DELETE ++++')
