#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time as timelib

def arguments(arg_list):
    number_of_arguments = len(arg_list)
    try:
        if (arg_list[1]!='-il' or arg_list[3]!='-ol' or arg_list[5]!='-op' or number_of_arguments!=7):
            raise ValueError("Correct use is 'python3 log_preprocessor.py  -il <input-log-file> -ol <output-preprocessed-log> -op <output-preprocessed_pages>'")
    except:
        raise ValueError("Correct use is 'python3 log_preprocessor.py  -il <input-log-file> -ol <output-preprocessed-log> -op <output-preprocessed_pages>'") from None
         
    print("     Input log filename: %s"%arg_list[2])
    print("     Output log filename: %s"%arg_list[4])
    print("     Output pages filename: %s"%arg_list[6])
    return arg_list[2], arg_list[4], arg_list[6];
        
def urls_database(log_dataframe, urls):
    start = timelib.time()
    log_requested_urls_notin_urldata=len(log_dataframe[~log_dataframe.requested_url.isin(urls.url)].requested_url.unique())
    print("     Requested URLs from log not in URL database: %d"%log_requested_urls_notin_urldata)
    log_referrer_urls_notin_urldata=len(log_dataframe[~log_dataframe.referrer_url.isin(urls.url)].referrer_url.unique())
    print("     Referrer URLs from log not in URL database: %d"%log_referrer_urls_notin_urldata)
    url_categories=len(urls.category.unique())
    url_topics=len(urls.topic.unique())    
    print("     Number of categories: %d"%url_categories)
    print("     Number of topics: %d"%url_topics)
    print("     URLs checked in %0.2f seconds."%(timelib.time()-start))
    return;  
    
def pages_database(log_dataframe, output_pages):
    start = timelib.time()
    log_requested_pages_notin_pagesdata=len(log_dataframe[~log_dataframe.requested_pageID.isin(output_pages.pageID)].requested_pageID.unique())
    print("     Requested pages from log not in pages database: %d"%log_requested_pages_notin_pagesdata)
    log_referrer_pages_notin_urldata=len(log_dataframe[~log_dataframe.referrer_pageID.isin(output_pages.pageID)].referrer_pageID.unique())
    print("     Referrer pages from log not in pages database: %d"%log_referrer_pages_notin_urldata)
    print("     Pages checked in %0.2f seconds."%(timelib.time()-start))
    return;
    
    
    
    
    
    
    
    

