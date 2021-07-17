import json
import pandas as pd
import pymysql


def get_data(table_name, url):
    f = open(url, encoding='utf-8')
    header_json, header_csv = get_json_head(table_name)
    data_pd = pd.DataFrame()
    data_pd = data_pd.append([header_csv])
    data_pd.to_csv('../data/Yelp/yelp_{0}.csv'.format(table_name), index=False, header=False, mode='a')
    while True:
        json_str = f.readline()
        row = get_json_content(header_json, json_str)
        print(row)
        data_pd = pd.DataFrame()
        data_pd = data_pd.append([row])
        data_pd.to_csv('../data/Yelp/yelp_{0}.csv'.format(table_name), index=False, header=False, mode='a')
        if not json_str:  # 到 EOF，返回空字符串，则终止循环
            break


def get_data_special(table_name, url):
    f = open(url, encoding='utf-8')
    # 连接数据库
    mysql_conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='926049', db='srut_poi_rec')
    while True:
        json_str = f.readline()
        json_content = json.loads(json_str)
        if table_name == 'user':
            # 探索关系
            user_id_1 = json_content.get('user_id')
            print(user_id_1)
            batch_rows = []
            friend_str = json_content.get('friends')
            # 若朋友不为空(None)
            if friend_str != 'None':
                friends = friend_str.split(', ')
                insert_sentense = 'insert into yelp_user_relation(user_id_1,user_id_2) ' \
                                  'values(%s,%s)'
                for friend in friends:
                    # 该朋友是否在库中
                    select_sentense_former = "select user_id from yelp_user where user_id=\'{0}\'".format(friend)
                    len_result = 0
                    try:
                        with mysql_conn.cursor() as cursor:
                            cursor.execute(select_sentense_former)
                            select_result_former = cursor.fetchall()
                            len_result = len(select_result_former)
                        mysql_conn.commit()
                    except Exception as e:
                        mysql_conn.rollback()
                    if len_result != 0:
                        # row = [user_id_1, friend]
                        # print(row)
                        # 先查询是否已经存在反向关系
                        select_sentense = 'select * from yelp_user_relation ' \
                                          'where user_id_1=\'{1}\' and user_id_2=\'{0}\''.format(user_id_1, friend)
                        try:
                            with mysql_conn.cursor() as cursor:
                                cursor.execute(select_sentense)
                                select_result = cursor.fetchall()
                                if len(select_result) == 0:
                                    batch_rows.append((user_id_1, friend))
                            mysql_conn.commit()
                        except Exception as e:
                            mysql_conn.rollback()
                print(batch_rows)
                try:
                    with mysql_conn.cursor() as cursor:
                        cursor.executemany(insert_sentense, batch_rows)
                    mysql_conn.commit()
                except Exception as e:
                    mysql_conn.rollback()
        if table_name == 'business':
            # 所属类别
            business_id = json_content.get('business_id')
            categories = json_content.get('categories').split(', ')
            for category in categories:
                select_sentense = 'select id from yelp_categories where name=\'{0}\''.format(category)
                insert_sentense = 'insert yelp_categories(name) values(\'{0}\')'.format(category)
                print(select_sentense)
                print(insert_sentense)
                try:
                    with mysql_conn.cursor() as cursor:
                        cursor.execute(select_sentense)
                        select_result = cursor.fetchall()
                        id = -1
                        if len(select_result) == 0:
                            cursor.execute(insert_sentense)
                            id = cursor.lastrowid
                        else:
                            id = select_result[0][0]
                        insert_relation = 'insert yelp_business_categories_relation(business_id,categories_id) ' \
                                          'values(\'{0}\',\'{1}\')'.format(business_id,id)
                        cursor.execute(insert_relation)
                    mysql_conn.commit()
                except Exception as e:
                    mysql_conn.rollback()
        if table_name == 'checkin':
            # 签到记录
            business_id = json_content.get('business_id')
            dates = json_content.get('date').split(', ')
            batch_rows = []
            insert_sentense = 'insert into yelp_checkin(business_id,user_id,date) values(%s,%s,%s)'
            for datetime in dates:
                # 拆分为年 月 日 时 分 秒 且至少精确到时
                dl1 = datetime.split(' ')
                # Y-M-d
                date = dl1[0]
                # HH:mm:ss
                time = dl1[1]
                dl2 = time.split(':')
                # 取时
                time_h = dl2[0]
                # 取分
                time_m = dl2[1]
                # 新查询条件
                new_date = date + ' ' + time_h
                select_sentense = 'select user_id from yelp_reviews where business_id=\'{0}\' and date_format(date,\'%Y-%m-%d %H\')=\'{1}\''.format(business_id,new_date)
                print(select_sentense)
                try:
                    with mysql_conn.cursor() as cursor:
                        cursor.execute(select_sentense)
                        select_result = cursor.fetchall()
                        if len(select_result)>0:
                            if len(select_result)==1:
                                user_id = select_result[0][0]
                                batch_rows.append((business_id,user_id,datetime))
                            else:
                                new_date += ':' + time_m
                                select_sentense = 'select user_id from yelp_reviews where business_id=\'{0}\' and date_format(date,\'%Y-%m-%d %H\')=\'{1}\''.format(business_id,new_date)
                                cursor.execute(select_sentense)
                                select_result = cursor.fetchall()
                                if len(select_result)==1:
                                    user_id = select_result[0][0]
                                    batch_rows.append((business_id,user_id,datetime))
                    mysql_conn.commit()
                except Exception as e:
                    mysql_conn.rollback()
            print(batch_rows)
            try:
                with mysql_conn.cursor() as cursor:
                    cursor.executemany(insert_sentense, batch_rows)
                mysql_conn.commit()
            except Exception as e:
                mysql_conn.rollback()
        if not json_str:  # 到 EOF，返回空字符串，则终止循环
            break
    mysql_conn.close()


def get_json_head(table_name):
    header_json, header_csv = None, None
    if table_name == 'business':
        header_json = ['business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude',
                       'stars', 'review_count', 'is_open']
        header_csv = ['business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude',
                      'stars', 'review_count', 'is_open']
    if table_name == 'review':
        header_json = ['review_id', 'user_id', 'business_id', 'stars', 'date', 'text', 'useful', 'funny',
                       'cool']
        header_csv = ['review_id', 'user_id', 'business_id', 'stars', 'date', 'text', 'useful', 'funny',
                      'cool']
    if table_name == 'tip':
        header_json = ['business_id', 'user_id', 'text', 'date', 'compliment_count']
        header_csv = ['business_id', 'user_id', 'text', 'date', 'compliment_count']
    if table_name == 'checkin':
        header_json = ['business_id', 'date']
        header_csv = ['business_id', 'date']
    if table_name == 'user':
        header_json = ['user_id', 'name', 'review_count', 'yelping_since', 'useful', 'funny', 'cool', 'fans',
                       'average_stars',
                       'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list',
                       'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny',
                       'compliment_writer',
                       'compliment_photos']
        header_csv = ['user_id', 'name', 'review_count', 'yelping_since', 'useful', 'funny', 'cool', 'fans',
                      'average_stars',
                      'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list',
                      'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny', 'compliment_writer',
                      'compliment_photos']
    return header_json, header_csv


def get_json_content(header_json, json_str):
    json_content = json.loads(json_str)
    col_len = len(header_json)
    row = []
    for i in range(col_len):
        row.append(json_content.get(header_json[i]))
    return row


if __name__ == '__main__':
    # get_data('business', '../data/Yelp/yelp_academic_dataset_business.json')
    # get_data('tip', '../data/Yelp/yelp_academic_dataset_tip.json')
    # get_data('user', '../data/Yelp/yelp_academic_dataset_user.json')
    get_data_special('checkin', '../data/Yelp/yelp_academic_dataset_checkin.json')
