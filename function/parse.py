#!/usr/bin/env python3
# -*- coding: utf-8 -*-

monthdict={'Jan': 1,'Feb': 2,'Mar': 3,'Apr': 4,'May': 5,'Jun': 6,'Jul': 7,'Aug': 8,'Sep': 9,'Oct': 10,'Nov': 11,'Dic': 12}

import pandas as pd
import numpy  as np
import time as timelib
import re
import datetime

def date_time(timestamp):
    # Example log format: [04/Sep/2017:00:22:41
    year = int(timestamp.split("[")[1].split("/")[2].split(":")[0])
    month = monthdict[timestamp.split("[")[1].split("/")[1]]
    day = int(timestamp.split("[")[1].split("/")[0])
    hour = int(timestamp.split(":")[1])
    minute = int(timestamp.split(":")[2])
    second = int(timestamp.split(":")[3])
    return datetime.datetime(year=year,month=month,day=day,hour=hour,minute=minute,second=second)


phone = ['iPod','iPhone','Android','Windows Phone']
tablet = ['iPad','Tablet']
computer = ['Windows','Linux','Macintosh'] 

def device(log_dataframe):
    start = timelib.time()
    log_dataframe['device'] = str()
    for row in log_dataframe.itertuples():
        if row.agent != row.agent:
            continue
        if any(substring in row.agent for substring in phone):
            log_dataframe['device'].at[row.Index] = 'Phone'
            continue
        elif any(substring in row.agent for substring in tablet):
            log_dataframe['device'].at[row.Index] = 'Tablet'
            continue
        elif any(substring in row.agent for substring in computer):
            log_dataframe['device'].at[row.Index] = 'Computer'
        else:
            log_dataframe['device'].at[row.Index] = 'Other'    
    print("     %d of %d requests inspected (%.2f%%) in %.0f seconds."%(log_dataframe.shape[0], log_dataframe.shape[0], 100.0, timelib.time() - start))
    return log_dataframe;


            
def amp_project(log_dataframe):
    log_dataframe['requested_url']=log_dataframe['requested_url'].astype(str)
    log_dataframe['referrer_url']=log_dataframe['referrer_url'].astype(str)
    loop_keeper = 0
    total_loops = log_dataframe.shape[0]
    loop_tick = np.floor(0.01*total_loops)
    tick_counter = 0
    start_type_retrieval = timelib.time()
    amp_project_pages_counter = 0
    for row in log_dataframe.itertuples():

        loop_keeper +=1
        if loop_keeper%loop_tick==0:
            tick_counter += 1
            print("	%d of %d requests inspected (%.2f%%) in %.0f seconds."%(tick_counter*loop_tick, total_loops, 100.0*tick_counter*loop_tick/total_loops,timelib.time()-start_type_retrieval), end='\r')

        # pass if NaN
        if row.referrer_url!=row.referrer_url:
            continue

        # pass if the string is short
        if len(row.referrer_url)<20:
            continue

        # checking if it is a AMP Project page
        if row.referrer_url[:20]!="https://www-melty-fr":
            continue

        splitted = row.referrer_url.split("/")

        # checking if it's just the main melty page at AMP
        if row.referrer_url=='https://www-melty-fr.cdn.ampproject.org/':
            log_dataframe['referrer_url'].at[row.Index] = "http://www.melty.fr/"
            amp_project_pages_counter+=1
            continue

        if len(splitted)>=8:
            if splitted[2]=='www-melty-fr.cdn.ampproject.org' and splitted[5]=='www.melty.fr' or splitted[6]=='amp':
                log_dataframe['referrer_url'].at[row.Index] = "http://www.melty.fr/"+splitted[7].split("?")[0]
                amp_project_pages_counter+=1
                continue
            if splitted[2]=='cdn.ampproject.org' and splitted[5]=='www.melty.fr' or splitted[6]=='amp':
                log_dataframe['referrer_url'].at[row.Index] = "http://www.melty.fr/"+splitted[7].split("?")[0]
                amp_project_pages_counter+=1
                continue
        
    print("	%d of %d requests inspected (%.2f%%) in %.0f seconds."%(total_loops, total_loops,100.0,timelib.time()-start_type_retrieval), end='\r')
    print("\n     %d requests containing AMP Project's URLs parsed into www.melty.fr addresses."%amp_project_pages_counter)
    log_dataframe['requested_url']=log_dataframe['requested_url'].apply(lambda x: '/'.join(x.split('/amp/')) if x.find('/amp/')>0 else x)
    log_dataframe['requested_url']=log_dataframe['requested_url'].apply(lambda x: 'www.melty.fr/'+x.split('www.google.com/amp/s/www.melty.fr/amp/')[1] if x.find('www.google.com/amp/s/www.melty.fr/amp/')>0 else x)
    log_dataframe['referrer_url']=log_dataframe['referrer_url'].apply(lambda x: '/'.join(x.split('/amp/')) if x.find('/amp/')>0 else x)
    log_dataframe['referrer_url']=log_dataframe['referrer_url'].apply(lambda x: 'www.melty.fr/'+x.split('www.google.com/amp/s/www.melty.fr/amp/')[1] if x.find('www.google.com/amp/s/www.melty.fr/amp/')>0 else x)

    return log_dataframe;


def query_string_filter(log_dataframe):

    loop_keeper = 0
    total_loops = log_dataframe.shape[0]
    loop_tick = int(np.floor(0.01*total_loops))
    tick_counter = 0
    start_type_retrieval = timelib.time()
    for row in log_dataframe.itertuples():

        loop_keeper +=1
        if loop_keeper%loop_tick==0:
            tick_counter += 1
            print("     %d of %d requests inspected (%.2f%%) in %.0f seconds."%(tick_counter*loop_tick, total_loops, 100.0*tick_counter*loop_tick/total_loops,timelib.time()-start_type_retrieval),end='\r')

        # if there are no query strings, there's nothing to do
        if row.requested_url.find('?')<0 and row.referrer_url.find('?')<0:
            continue

        # TREATMENT OF THE REFERRER URL
        # If there's a query string in the referrer URL
        if row.referrer_url.find('?')>0:
            # Check for cases where we can delete it. For now, it's always:
            log_dataframe['referrer_url'].at[row.Index]=row.referrer_url.split('?')[0]

        # TREATMENT OF THE REQUESTED URL
        # If there's a query string in the requested URL
        if row.requested_url.find('?')>0:
            requrl=row.requested_url

            # Try to see if we can guess referrer URL when absent
            if row.referrer_url=='Unknown URL':
                if requrl.find('?utm_source=')>0:
                    # there's source information, try to infer referrer URL
                    # we try to retrieve the source tag
                    try:
                        source_tag = requrl[requrl.find('?utm_source=')+12:requrl.find('&')]
                        if any(substring in source_tag for substring in ['facebook','Facebook']):
                            log_dataframe['referrer_url'].at[row.Index]='www.facebook.com'
                        if any(substring in source_tag for substring in ['google','Google']):
                            log_dataframe['referrer_url'].at[row.Index]='www.google.com'
                        if any(substring in source_tag for substring in ['twit']):
                            log_dataframe['referrer_url'].at[row.Index]='www.twitter.com'
                        if source_tag=='automatic':
                            log_dataframe['referrer_url'].at[row.Index]='email'
                    except:
                        print('WARNING: Unanticipated utm_source tag format.\n')
            # Now we can delete the cases that do not convey relevant information
            if not(row.requested_url.find('?id_article=')>0 or row.requested_url.find('?id_live=')>0):
                log_dataframe['requested_url'].at[row.Index]=row.requested_url.split('?')[0]
            # Detecting and treating the case www.melty.fr/something-aCODE.html?id_article=CODE
            if len(re.findall('a\d+\.html',row.requested_url))==1 and row.requested_url.find('?id_article=')>0:
                log_dataframe['requested_url'].at[row.Index]=row.requested_url.split('?')[0]
    print("     %d of %d requests inspected (%.2f%%) in %0.2f seconds."%(total_loops, total_loops, 100.0,timelib.time()-start_type_retrieval))
    return log_dataframe;

def referrerurlparser(urlstring):

	if not(isinstance(urlstring, str)):
		return 'Unknown URL'
	if urlstring=='-' or urlstring==' ' or len(urlstring)==0 or urlstring=='http://':
		return 'Unknown URL'
	if len(urlstring.split('//'))>=2:
		return urlstring.split('//')[1]
	else:
		return urlstring;

def urls(log_dataframe):
    start = timelib.time()
    log_dataframe['requested_url']=log_dataframe['requested_url'].apply(lambda x: x.split(' ')[1].split('//')[1])
    log_dataframe['referrer_url'] = log_dataframe['referrer_url'].apply(lambda x: referrerurlparser(x))
    log_dataframe['requested_url'] = log_dataframe['requested_url'].apply(lambda x: x[0:-1] if x[-1]=="/" else x)
    log_dataframe['referrer_url'] = log_dataframe['referrer_url'].apply(lambda x: x[0:-1] if x[-1]=="/" else x)
    log_dataframe['requested_url'] = log_dataframe['requested_url'].apply(lambda x: x[0:-4] if x.endswith('/no') else x)
    log_dataframe['referrer_url'] = log_dataframe['referrer_url'].apply(lambda x: x[0:-4] if x.endswith('/no') else x)
    log_dataframe['simple_referrer_url'] = log_dataframe['referrer_url'].apply(lambda x: (x.split('/')[0] if len(x.split('/'))>=2 else x) if  x!='Unknown URL' else x)
    log_dataframe=query_string_filter(log_dataframe)
    print("     URLs parsed in %0.2f seconds."%(timelib.time()-start))
    return log_dataframe;
