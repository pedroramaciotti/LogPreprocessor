#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import time as timelib

# File with string to match in agent field in log file during the search for bots
bot_stringmatch_filename = "robot_agent_stringmatch.txt"
# C1 : Users that made too many requests in a minute
C1_max_requests_per_minute = 20
# C2 : Users that made too many requests in an hour
C2_max_requests_per_hour = 15*60
# C3 : Users that never had an hour of inactivity
C3_max_hours_active = 20

def declared_bot_filter(log_dataframe):

    robot_users = set([])

    print("     Considering strings from %s to match for bots in 'agent' field."%bot_stringmatch_filename)

    with open(bot_stringmatch_filename) as f:
        bot_list = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    bot_list = [x.strip() for x in bot_list]


    log_dataframe['declared_robot']=pd.Series(np.full(log_dataframe.shape[0],False,dtype=bool)).values
#    start = timelib.time()
    loop_keeper = 0
    total_loops = log_dataframe.shape[0]
    loop_tick = np.floor(0.01*total_loops)
    tick_counter = 0
    start_type_retrieval = timelib.time()
    for row in log_dataframe.itertuples():
        loop_keeper +=1
        if loop_keeper%loop_tick==0:
            tick_counter += 1
            print("	%d of %d requests inspected (%.2f%%) in %.0f seconds."%(tick_counter*loop_tick, total_loops, 100.0*tick_counter*loop_tick/total_loops,timelib.time()-start_type_retrieval),end='\r')
        if row.agent!=row.agent:
            continue
        if any(substring in row.agent for substring in bot_list):
            log_dataframe['declared_robot'].at[row.Index]=True
            robot_users.add(row.user)
    print("	%d of %d requests inspected (%.2f%%) in %.0f seconds."%(total_loops, total_loops, 100.0,timelib.time()-start_type_retrieval),end='\r')
    print("\n 	Deleting %d requests from %d self-declared robots."%(log_dataframe[log_dataframe.declared_robot].shape[0],len(robot_users)))
    filtered_log = log_dataframe[~log_dataframe.declared_robot]
    output_log=filtered_log.drop(['declared_robot'],axis=1)

    return output_log;

def suspicious_activity_filter(log_dataframe):

    print("     Considering requests made by users with suspicious activity...")

    log_dataframe['absolute_hour'] = log_dataframe['timestamp'].apply(lambda x: x.day*24 + x.hour).values
#    log_dataframe['minute'] = log_dataframe['timestamp'].apply(lambda x: x.minute).values
    log_dataframe['absolute_minute'] = log_dataframe['timestamp'].apply(lambda x: x.day*24*60 + 60*x.hour + x.minute).values
    log_dataframe['requests'] = pd.Series(np.ones(log_dataframe.shape[0],dtype=int)).values

    users = log_dataframe['user']
    users = users[~users.duplicated()]

    urls = log_dataframe['requested_url']
    urls = urls[~(urls.duplicated())]

    print("        Selecting users that made more than %d requests in a minute..."%C1_max_requests_per_minute)
    C1_users_minutely_activity = log_dataframe[['user','absolute_minute','requests']].groupby(['user','absolute_minute']).count()
    C1_users_ranked_by_max_minute_activity = C1_users_minutely_activity.groupby('user').max().sort_values(axis=0, ascending=False,by='requests')
    C1_users = C1_users_ranked_by_max_minute_activity[C1_users_ranked_by_max_minute_activity['requests']>C1_max_requests_per_minute].index.values

    print("        Selecting users that made more than %d requests in an hour..."%C2_max_requests_per_hour)
    C2_users_hourly_activity = log_dataframe[['user','absolute_hour','requests']].groupby(['user','absolute_hour']).count()
    C2_users_ranked_by_max_hourly_activity = C2_users_hourly_activity.groupby('user').max().sort_values(axis=0, ascending=False,by='requests')
    C2_users = C2_users_ranked_by_max_hourly_activity[C2_users_ranked_by_max_hourly_activity['requests']>C2_max_requests_per_hour].index.values

    unwanted_users = list(set(C1_users)|set(C2_users))
    filtered_log = log_dataframe[~log_dataframe['user'].isin(unwanted_users)]

    print("        Deleting %d requests made by %d suspicious users."%(log_dataframe[log_dataframe['user'].isin(unwanted_users)].shape[0],len(unwanted_users)))

    output_log=filtered_log.drop(['absolute_hour','absolute_minute','requests'],axis=1)

    return output_log;
