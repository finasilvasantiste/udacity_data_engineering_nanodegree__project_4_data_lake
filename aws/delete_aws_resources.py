from aws.aws_cluster_types.EMRCluster import EMRCluster


def delete_aws_resources():
    """
    Deletes all aws resources.
    :return:
    """
    emr_cluster = EMRCluster()
    emr_cluster.delete_resources()


if __name__ == "__main__":
    delete_aws_resources()
