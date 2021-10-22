import difflib


def similar_queries(connect, match_percentage=0.8):
    query_list = connect.query('dwh', """SELECT QUERY_ID, QUERY_TEXT FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
        WHERE EXECUTION_STATUS = 'RUNNING' AND USER_NAME NOT IN ('TEAMCITY', 'POWERBI', 'ETL', 'ETL_EMERGENCY')""")
    matcher = difflib.SequenceMatcher() #создаем экземпляр класса SequenceMatcher
    list4kill = []
    for q in query_list:
        matcher.set_seq2(q[1]) #устанавливаем первую последовательность для сравнения
        for q2 in query_list:
            if q[1] == q2[1]:
                continue
            matcher.set_seq1(q2[1]) #устанавливаем вторую последовательность для сравнения
            if matcher.ratio() > match_percentage:
                list4kill.append(q2[0])
    return list4kill


def max_time_running(connect):
    max_ti_query_history = connect.query('dwh', """SELECT QUERY_ID FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
        WHERE EXECUTION_STATUS = 'RUNNING' AND USER_NAME NOT IN ('TEAMCITY', 'POWERBI', 'ETL', 'ETL_EMERGENCY') ORDER BY execution_time DESC LIMIT 1""")
    return max_ti_query_history[0][0]


def if_the_queue(connect):
    queue_query = connect.query('dwh', """SELECT COUNT(*) FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
            WHERE EXECUTION_STATUS = 'QUEUED' AND USER_NAME NOT IN ('TEAMCITY', 'POWERBI', 'ETL', 'ETL_EMERGENCY')""")
    return queue_query[0][0]


def the_queue(connect):
    #находим похожие запросы и убиваем их
    list = similar_queries(connect)
    for query_id in list:
        connect.query('dwh', f"""SELECT system$cancel_query('{query_id}')""")
    #проверяем, если очередь
    queue = if_the_queue(connect)
    #если есть, то убиваем пока очередь не закончиться

    while queue > 0:
        longest_query = max_time_running(connect)
        connect.query('dwh', f"""SELECT system$cancel_query('{longest_query}')""")
        queue = if_the_queue(connect)


if __name__ == '__main__':
    the_queue(connect)
