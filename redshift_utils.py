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