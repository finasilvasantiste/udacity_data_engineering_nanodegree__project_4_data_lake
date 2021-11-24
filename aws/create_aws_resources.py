from aws.aws_cluster_types.EMRCluster import EMRCluster


def create_aws_resources():
    """
    Creates all necessary aws resources.
    :return:
    """
    emr_cluster = EMRCluster()
    emr_cluster.create_resources()

if __name__ == "__main__":
    create_aws_resources()
