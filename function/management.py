#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import re
import time as timelib

pd.options.mode.chained_assignment = None

def retrieve_article_id(urlstring):

	# Looking for 'id_article' string
	id_position = urlstring.find('id_article=')
	if id_position>0:
		return (urlstring.split('id_article=')[1]);
	match = re.findall('a\d+\.html',urlstring)
	if len(match)==1:
		return (match[0].split('a')[1].split('.')[0])
	match = re.findall('actu\d+\.html',urlstring)
	if len(match)==1:
		return (match[0].split('actu')[1].split('.')[0])
	return '';

def extract(log_dataframe):
    urls = pd.DataFrame(columns = ["url","article_code"], dtype=str)
    requested_urls = log_dataframe['requested_url']
    referrer_urls = log_dataframe['referrer_url']
    urls['url'] = pd.concat([requested_urls,referrer_urls], axis = 0)
    urls.drop_duplicates(subset=['url'],inplace=True)
    urls.reset_index(inplace=True,drop=True)
    urls['article_code']=urls['url'].apply(retrieve_article_id)
    urls_with_code=urls[urls.article_code!=''].shape[0]
    print("     %d unique URLs found in the log file (%.1f%% of which are articles)."%(urls.shape[0],100*urls_with_code/urls.shape[0]))
    return urls;

def duplicity(urls, log_dataframe):
    urls['short_format']=urls.url.apply(lambda x: x.find('?id_article=')>0)
    grouped_articles=urls.groupby('article_code')
    duplicated_articles=grouped_articles.count()
    duplicated_articles=duplicated_articles[duplicated_articles.url>1]
    duplicated_articles_lookup_table=pd.Series()
    for code in duplicated_articles.index:
        #
        if code=='':
            continue

        group=grouped_articles.get_group(code)
        # skip if there all short or all long format
        if group.short_format.all() or not group.short_format.any():
            continue
        # add to lookup table

        short_url=group.url[group.short_format].iloc[0]
        long_url=group.url[~group.short_format].iloc[0]
        duplicated_articles_lookup_table=duplicated_articles_lookup_table.append(pd.Series([long_url],index=[short_url]))
        # delete the short format copy from new urls
        urls=urls[urls.url!=short_url]

    # changing detected URL with duplicity in the log table
    log_dataframe[log_dataframe.requested_url.isin(duplicated_articles_lookup_table.index)].requested_url=log_dataframe[log_dataframe.requested_url.isin(duplicated_articles_lookup_table.index)].requested_url.map(duplicated_articles_lookup_table, na_action='ignore')
    log_dataframe.loc[log_dataframe.requested_url.isin(duplicated_articles_lookup_table.index),'requested_url']=log_dataframe[log_dataframe.requested_url.isin(duplicated_articles_lookup_table.index)].requested_url.map(duplicated_articles_lookup_table, na_action='ignore')
    urls.drop(['short_format'],inplace=True,axis=1)
    return urls;

def format_pages(urls):
    start = timelib.time()
    urls['pageID']=str()
    urls['external']=str()
    urls['folderID']=str()
    for i, row in enumerate(urls.itertuples()):
        # Anonymise requests
        urls['pageID'].at[row.Index] = 'PAGE'+str(i)
        # Check if the requests are from Melty
        if urls['id'].at[row.Index][0] == 'M' :
            urls['external'].at[row.Index] = 'False'
        else : urls['external'].at[row.Index] ='True'
    # Anonymise folders
    folder = list()
    for i in range(len(urls.melty_folder_id.unique())):
        folder.append('FLDR'+str(i))
    urls['folderID'] = urls.melty_folder_id.map(pd.Series(data=folder,index=urls.melty_folder_id.unique()))
    print("     URLs formatted in %0.2f seconds."%(timelib.time()-start))
    return urls;

def format_log(log_dataframe, urls):
    start = timelib.time()
    # Anonymise users
    log_dataframe['userID'] = str()
    user = list()
    for i in range(len(log_dataframe.user.unique())):
        user.append('USR'+str(i))
    log_dataframe['userID'] = log_dataframe.user.map(pd.Series(data=user,index=log_dataframe.user.unique()))
    # Identity urls with pageID to anonymise
    log_dataframe['requested_pageID'] = 'Unknown'
    log_dataframe['referrer_pageID'] = 'Unknown'
    log_dataframe['requested_pageID'] = log_dataframe.requested_url.map(pd.Series(index = urls.url.values, data = urls.pageID.values))
    log_dataframe['referrer_pageID'] = log_dataframe.referrer_url.map(pd.Series(index = urls.url.values, data = urls.pageID.values))
    # Select useful entries
    output_log = pd.DataFrame(columns = ['userID','timestamp','requested_pageID','referrer_pageID','device'], index=None)
    output_log['userID'] = log_dataframe['userID']
    output_log['timestamp'] = log_dataframe['timestamp']
    output_log['requested_pageID'] = log_dataframe['requested_pageID']
    output_log['referrer_pageID'] = log_dataframe['referrer_pageID']
    output_log['device'] = log_dataframe['device']
    print("     Output log formatted in %0.2f seconds."%(timelib.time()-start))
    return output_log;
