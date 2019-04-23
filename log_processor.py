#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sys
import time as timelib
import function.filter as filtering
import function.parse as parsing
import function.management as managing
import function.url_database_manager as m_database
import function.check as checking

# Checking filenames
print("\n   * Checking filenames...")
arg_list = sys.argv
input_log_filename, preprocessed_log_filename, preprocessed_pages_filename = checking.arguments(arg_list)

# Reading the log file
print("\n   * Reading log file from %s ..."%input_log_filename)
start = timelib.time()
columns_name = ['user','timestamp','timezone','requested_url','response','size','referrer_url','agent']
log_dataframe = pd.read_csv(input_log_filename, sep = ' ', header = None, 
                            names = columns_name, 
                            dtype = {'user':str,'timestamp':str,'timezone':str,'requested_url':str,'response':str,'size':str,'referrer_url':str,'agent':str})
initial_number_of_entries = log_dataframe.shape[0]
print("     File read in %0.2f seconds."%(timelib.time()-start))
print("     Initial log entries: %d."%initial_number_of_entries)

# Dropping duplicated requests
print("\n   * Dropping repeated entries in the log (possibly coming from file merging)...")
start = timelib.time()
log_dataframe.drop_duplicates(inplace=True)
current_number_of_entries=log_dataframe.shape[0]
print("     Deleted %d repeated log entries (%.1f%%)."%(initial_number_of_entries-current_number_of_entries,100*(initial_number_of_entries-current_number_of_entries)/initial_number_of_entries))

# Dropping requests denied by the server
print("\n   * Dropping the requests denied by the server...")
start = timelib.time()
denied_request_entries=log_dataframe[log_dataframe.response.apply(lambda x: not x.startswith('2')) ].shape[0]
log_dataframe = log_dataframe[log_dataframe.response.apply(lambda x: x.startswith('2'))]    
print("     Deleted %d log entries (%.1f%%)."%(denied_request_entries,100*denied_request_entries/initial_number_of_entries))  

# Parsing the date
print("\n   * Parsing the timestamps in the log ...")
log_dataframe['timestamp'] = log_dataframe['timestamp'].apply(lambda x: parsing.date_time(x))
if len(log_dataframe.timezone.unique()) != 1:
    raise AssertionError("\n     ERROR: There is more than one timezone, but timezone adjustment is still unimplemented!\n")
print("     Log streching from %s to %s."%(str(log_dataframe['timestamp'].min()),str(log_dataframe['timestamp'].max())))

# Filter bot requests
print("\n   * Filtering requests from bots ...")
log_dataframe = filtering.declared_bot_filter(log_dataframe)
log_dataframe = filtering.suspicious_activity_filter(log_dataframe)
print("     Remaining log entries: %d (%.1f%% of initial entries)."%(log_dataframe.shape[0],100*log_dataframe.shape[0]/initial_number_of_entries))

# Parsing the device
print("\n   * Parsing the device used...")
log_dataframe = parsing.device(log_dataframe)

# Parsing the urls from AMP Project
print("\n   * Parsing the URLs from AMP project in the log...")
log_dataframe = parsing.amp_project(log_dataframe)

# Parsing the urls
print("\n   * Parsing the URLs in the log.")
log_dataframe = parsing.urls(log_dataframe)

# Analizing users
print("\n   * Retrieving unique users ...")
start = timelib.time()
users = log_dataframe['user']
users.drop_duplicates(inplace=True)
print("     %d unique users retrieved in %0.2f seconds."%(users.size,timelib.time()-start))

# Extracting urls
print("\n   * Extracting unique URLs addresses...")
urls = managing.extract(log_dataframe)

# Resolving duplicity in urls
print("\n   * Resolving duplicity in urls...")
urls = managing.duplicity(urls, log_dataframe)

# Url Database Management: request topic and category of each url 
print("\n   * Request to Melty the topic and category of each url... ")
urls = m_database.create_new_database(urls)

# Some checks about urls 
print("\n   * Checking urls database...")
checking.urls_database(log_dataframe,urls)

# Formatting pages: anonymise with topic and category for each url
print("\n   * Formatting urls ...")
urls = managing.format_pages(urls)

# Formatting output log:  anonymise page and user and select useful entries
print("\n   * Formatting output log...")
output_log = managing.format_log(log_dataframe,urls)

# Formatting output pages
print("\n   * Formatting output pages...")
output_pages = urls[['pageID','category','topic','external']]

# Some checks about pages
print("\n   * Checking pages database...")
checking.pages_database(log_dataframe,output_pages)

# Saving Data
print("\n   * Saving data to file...")
print("     Saving processed log file to %s"%preprocessed_log_filename)
output_log.to_csv(preprocessed_log_filename,sep=',',header= True,index=None)
print("     Saving pages' list log file to %s\n\n"%preprocessed_pages_filename)
output_pages.to_csv(preprocessed_pages_filename,sep=',',header= True,index = None)







