def execute_sql_queries(cur, conn, q_list):
    """
    Execute a list of queries.

    Args:
        conn: (connection) instance of connection class
        cur: (cursor) instance of cursor class
        q_list: list of queries

    Returns:
        none
    """
    for query in q_list:
        cur.execute(query)
        conn.commit()

def delete_cluster(role, cluster):
    """
    Using information in config file, deletes clusters and roles.
    
    Args:
        role: IAM Role Name
        cluster: Cluster Identifier

    Returns:
        none
    """
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)