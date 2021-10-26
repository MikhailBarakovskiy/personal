import difflib
import requests
import time

usernames = "'TEAMCITY'"


def send_slack_message(message):
    payload = '{"text": "%s"}' % message
    responce = requests.post(URL,
                             data=payload)


def get_dictionary_of_user_queries(connect):
    dict_of_user_queries = {}
    table_query_history = connect.query('dwh', """SELECT USER_NAME, QUERY_ID, QUERY_TEXT
    FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
    WHERE EXECUTION_STATUS = 'RUNNING' OR EXECUTION_STATUS = 'QUEUED' ORDER BY USER_NAME, START_TIME""")
    for row in table_query_history:
        if row[0] not in dict_of_user_queries:
            dict_of_user_queries[row[0]] = [[row[1], row[2]]]
        else:
            dict_of_user_queries[row[0]] += [[row[1], row[2]]]

    return dict_of_user_queries


def get_similar_queries_id(query_list, match_percentage=0.80):
    matcher = difflib.SequenceMatcher()  # создаем экземпляр класса SequenceMatcher
    similar_queries_list = []
    for counter in range(len(query_list)):
        query_id_and_query_text_1 = query_list.pop()
        query_id = query_id_and_query_text_1[0]
        query_text = query_id_and_query_text_1[1]
        matcher.set_seq2(query_text)  # устанавливаем первую последовательность для сравнения
        for query_id_and_query_text_2 in query_list:
            query_text_2 = query_id_and_query_text_2[1]
            matcher.set_seq1(query_text_2)  # устанавливаем вторую последовательность для сравнения
            percent = matcher.ratio()
            if percent > match_percentage and query_id not in similar_queries_list:
                similar_queries_list.append(query_id)
                break
    return similar_queries_list


def get_longest_query_and_username(connect, names=usernames):
    max_ti_query_history = connect.query('dwh', f"""SELECT USER_NAME, QUERY_ID FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
        WHERE EXECUTION_STATUS = 'RUNNING' AND USER_NAME NOT IN ({names}) ORDER BY execution_time DESC LIMIT 1""")
    return max_ti_query_history[0]


def get_count_of_queries_in_queue(connect, names=usernames):
    queue_query = connect.query('dwh', f"""SELECT COUNT(*) FROM TABLE(UTIL_DB.information_schema.query_history(RESULT_LIMIT => 100)) 
            WHERE EXECUTION_STATUS = 'QUEUED' AND USER_NAME NOT IN ({names})""")
    return queue_query[0][0]


def kill_queries(connect):
    # получаем словарь с ключем юзер, а значения список с списками, где содержиться айди-запроса и текст запроса для сравнения
    user_query_dict = get_dictionary_of_user_queries(connect)
    # проходимся по словарю и проверяем, есть ли у пользователя похожие запросы
    for user in user_query_dict:
        similar_queries = get_similar_queries_id(user_query_dict[user])
        if similar_queries: # если есть, то присылаем оповещение в Слак и проходимся по списку с похожими запросами
            send_slack_message(f'User {user} runs similar queries :open_mouth:')
            # for query_id in similar_queries:
            #     connect.query('dwh', f"""SELECT system$cancel_query('{query_id}')""")

    # проверяем, есть ли очердь
    count_of_queries = get_count_of_queries_in_queue(connect)
    # если есть, то убиваем пока очередь не закончиться
    while count_of_queries > 0:
        username_and_id = get_longest_query_and_username(connect)
        send_slack_message(f'User {username_and_id[0]} makes query that are too long :rage:')
        #connect.query('dwh', f"""SELECT system$cancel_query('{username_and_id[1]}')""")
        time.sleep(2)
        count_of_queries = get_count_of_queries_in_queue(connect)

def test_q(connect):
    send_slack_message('Checking for the presence of queries queue :mag_right:')
    if get_count_of_queries_in_queue(connect) > 0:
        kill_queries(connect)
