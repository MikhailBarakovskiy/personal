import difflib

usernames = "'TEAMCITY', 'POWERBI', 'ETL', 'ETL_EMERGENCY'"


def get_similar_queries(connect, match_percentage=0.8, names=usernames):
    query_list = connect.query('dwh', f"""SELECT QUERY_ID, QUERY_TEXT FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
        WHERE EXECUTION_STATUS = 'RUNNING' AND USER_NAME NOT IN ({names})""")
    matcher = difflib.SequenceMatcher() #создаем экземпляр класса SequenceMatcher
    list4kill = []
    for query_text_1 in query_list:
        matcher.set_seq2(query_text_1[1]) #устанавливаем первую последовательность для сравнения
        for query_text_2 in query_list:
            if query_text_1[1] == query_text_2[1]:
                continue
            matcher.set_seq1(query_text_2[1]) #устанавливаем вторую последовательность для сравнения
            if matcher.ratio() > match_percentage:
                similar_query_id = query_text_2[0]
                list4kill.append(similar_query_id)
    return list4kill


def get_longest_query(connect, names=usernames):
    max_ti_query_history = connect.query('dwh', f"""SELECT QUERY_ID FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
        WHERE EXECUTION_STATUS = 'RUNNING' AND USER_NAME NOT IN ({names}) ORDER BY execution_time DESC LIMIT 1""")
    return max_ti_query_history[0][0]


def get_count_of_queries_in_queue(connect, names=usernames):
    queue_query = connect.query('dwh', f"""SELECT COUNT(*) FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
            WHERE EXECUTION_STATUS = 'QUEUED' AND USER_NAME NOT IN ({names})""")
    return queue_query[0][0]


def kill_queries(connect):
    #находим похожие запросы и убиваем их
    similar_queries_id = get_similar_queries(connect)
    for query_id in similar_queries_id:
        connect.query('dwh', f"""SELECT system$cancel_query('{query_id}')""")
    #проверяем, если очередь
    count_of_queries = get_count_of_queries_in_queue(connect)
    #если есть, то убиваем пока очередь не закончиться
    while count_of_queries > 0:
        longest_query = get_longest_query(connect)
        connect.query('dwh', f"""SELECT system$cancel_query('{longest_query}')""")
        count_of_queries = get_count_of_queries_in_queue(connect)


if __name__ == '__main__':
    kill_queries()
