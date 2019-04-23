#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time as timelib
import pandas as pd
import numpy as np
import urllib
import re
import requests
import json
#import sys

##################################
##################################
####                          ####
#### SCRAPPERS AND RETRIEVERS ####
####                          ####
##################################
##################################

agent_headers = {
    'User-Agent': 'LIP6 ALGODIV'
}

def melty_article_API_request(melty_id,test=False):
    # The dictionary to append to the tables
    data={'id':'', 'url':'', 'category':'','melty_page_type':'','article_API_consultation':'', 'page_scrap_consultation':'','melty_thema_name':''}
    if test:
        return data;
    # The URL for the API request
    url = "https://api.meltygroup.com/v1/fr/articles/%s"%(melty_id)
    try:
        response = requests.get(url, headers = agent_headers)
    except:
        data['article_API_consultation']='ERROR: Request failed.'
        return data;
    if response.status_code != 200:
        data['article_API_consultation']='ERROR: response not 200 : %d.'%response.status_code
        return data;
    try:
        json_data = json.loads(requests.get(url).text)
        data['melty_thema_name']=json_data['thema']['name']
        data['article_API_consultation']='AOK'
    except:
        data['article_API_consultation']='ERROR: response OK, JSON data not retrieved.'
    return data;

def french_decoding(string):
    for word, initial in {"\\u00e9":"é", "\\u00e8":"è", "\\u00e7": "ç", "\\u00e0":"à", "\\u00ea":"ê"}.items():
        string = string.replace(word.lower(), initial);
    return string;

def scrap_field(webp,field_name):

    position = webp.index(field_name)
    try:
        if webp[(position+len(field_name)+2):(position+len(field_name)+6)]=='null':
            field_value = 'null'
        else:
            field_value = french_decoding(webp[position:(position+50)].split("\"")[2])
    except:
        field_value = "Not retrieved"
    return field_value;

def scrap_numeric_field(webp,field_name):
    position = webp.index(field_name)
    try:
        if webp[(position+len(field_name)+2):(position+len(field_name)+6)]=='null':
            field_value = 'null'
        else:
            field_value = webp[position:(position+50)].split("\"")[1].split(":")[1].split(",")[0]
    except:
        field_value = "Not retrieved"
    return field_value;

def melty_nonarticle_scrap(url,test=False):
    # The dictionary to append to the tables
    data={'id':'', 'url':'', 'category':'','melty_page_type':'','article_API_consultation':'', 'page_scrap_consultation':'','melty_thema_name':''}
    if test:
        return data;
    try:
        req=urllib.request.Request('http://'+url, headers=agent_headers)
        webp = urllib.request.urlopen(req).read().decode('utf-8')
    except:
        data['page_scrap_consultation']='ERROR: HTML not retrieved.'
        return data;
    try:
        data['melty_page_type']=scrap_field(webp,'page_type')
        data['melty_thema_name']=scrap_field(webp,'thema_name')
        data['page_scrap_consultation']='AOK'
    except:
        data['page_scrap_consultation']='ERROR: Unexpected format.'
    return data;

############################################
############################################
####                                    ####
#### CATEGORY AND TOPIC CLASSIFICATION  ####
####                                    ####
############################################
############################################

def mna_page_type_classificator(category):
    if category=='actu':
        return 'article';
    if category=='coms':
        return 'forum';
    if category=='concours':
        return 'contest';
    if category=='galerie':
        return 'gallery';
    if category in ['world','folder']:
        return 'sub-topic page';
    if category=='thema':
        return 'topic page';
    if category in ['search', 'search-page']:
        return 'search page';
    if category in ['video', 'live','quiz','article']:
        return category;
    if category=='videos':
        return 'video';
    if category in ['profil','login', 'register']:
        return 'user page';
    return 'other';

def ext_url_classificator(url):

    if url.startswith(('www.melty','www.shoko.fr','www.fan2.fr','www.virginradio.fr','www.airofmelty.fr',
                       'www.mcm.fr','www.june.fr','www.tyramisu.fr')):
        return 'sister site';
    elif any([search_word in url for search_word in ['search','google','bing','ecosia','yahoo']]):
        return 'search page';
    elif any([social_word in url for social_word in ['facebook','twitter','pinterest','fb','tumblr','reddit','instagram','youtube']]):
        return 'social';
    else:
        return 'other';
    
def thema_mapper(thema):
    if thema in ['Télé','Emissions','Télévision']:
        return 'TV';
    if thema in ['Séries','Series','Ciné & Séries','Séries\\/Télé US','Séries / TV',
                 'Séries/Télé US','Séries \\/ TV','Série\\/Télé US','Série/Télé US']:
        return 'Series';        
    if thema in ['Célébrités','Stars & style','People','Social News','Celebs','Sociétés','C\\él\\ébrit\\és\\']:
        return 'Celebrities';
    if thema in ['Musique',]:
        return 'Music';
    if thema in ['Comics & Mangas','Mangas','Comics']:
        return 'Comic';
    if thema in ['Jeux-Vidéo','Games','VideoGames']:
        return 'VideoGames';
    if thema in ['Cinéma','Movies','Ciné']:
        return 'Movies';
    if (thema in ['null','Not retrieved','folder_id\\','']) or thema!=thema:
        return 'None';
    if thema in ['Sports',"Sports d'aventure",'Sports motorisés','Sports aquatiques',
                 "Sports d'hiver",'Sports urbains']:
        return 'Sport';
    if thema in ['Actu','Info', 'News','Infos']:
        return 'News';
    if thema in ['Sorties','Agenda','Événements','Bons plans','\\u00c9vénements','Évènements']:
        return 'Events';
    if thema in ['Mode','Beauté','Sapes stylées','Sneakers Spot','Lookbook','Bling-bling','Marques','Focus sur les looks','Tendances','Dressing girly']:
        return 'Look';
    if thema in ['Humour','Just for LOL','Humoristes','Se marrer']:
        return 'Humor';
    if thema in ['Food life','Food','fast Food','Food Porn','Restos']:
        return 'Food';
    if thema in ['Psycho - Sexo','Sexy Life','Love','Beau Gosse']:
        return 'LoveLife';
    if thema in ['Bien être','Vivre','Healthy Life']:
        return 'Wellbeing';
    if thema in ['Campus','Student Spirit']:
        return 'Student';
    if thema in ['High-tech','Geek tips','Sciences','High-Tech','Applis']:
        return 'Tech';
    return 'Other';


##################################
##################################
####                          ####
#### TABLE UPDATER            ####
####                          ####
##################################
##################################

def tables_updater(new_urls,mar,mna,ext,test=False):

    # Retrieving the current id counter for the 3 types of URLs
    if mar.shape[0]>0:
        mar_counter = mar.id.apply(lambda x: x[3:]).astype(int).max()
    else:
        mar_counter = 0
    if mna.shape[0]>0:
        mna_counter = mna.id.apply(lambda x: x[3:]).astype(int).max()
    else:
        mna_counter = 0
    if ext.shape[0]>0:
        ext_counter = ext.id.apply(lambda x: x[3:]).astype(int).max()
    else:
        ext_counter = 0

    print("     Retrieving the data for the detected unknown URLs...")
    loop_keeper = 0
    total_loops = new_urls.shape[0]
    loop_tick = np.floor(0.01*total_loops)
    tick_counter = 0
    start_time = timelib.time()
    for row in new_urls.itertuples():
        loop_keeper +=1
        if loop_keeper%loop_tick==0:
            tick_counter += 1
            print("     %d of %d URLs inspected (%.2f%%) in %.0f seconds."%(tick_counter*loop_tick, total_loops, 100.0*tick_counter*loop_tick/total_loops,timelib.time()-start_time),end='\r')

        appendable_url_dict = {}

        # Treat differently wether they are articles with (API) or not articles (scrapping)
        if row.article_code!='' and row.url.startswith('www.melty.fr'):
            # (mar) The URL is from a Melty article with ID that we can use to lookup in the API
            mar_counter += 1
            appendable_url_dict=melty_article_API_request(row.article_code,test)
            appendable_url_dict['id']='MAR'+str(mar_counter)
            appendable_url_dict['url']=row.url
            appendable_url_dict['category']= 'article'
            mar=mar.append(appendable_url_dict,ignore_index=True)

        elif row.url.startswith('www.melty.fr'):
            # (mna) The URL is from a non-article Melty page, we crawl/scrap for the data
            mna_counter += 1
            appendable_url_dict=melty_nonarticle_scrap(row.url,test)
            appendable_url_dict['id']='MNA'+str(mna_counter)
            appendable_url_dict['url']=row.url
            appendable_url_dict['category']=mna_page_type_classificator(appendable_url_dict['melty_page_type'])
            mna=mna.append(appendable_url_dict,ignore_index=True)
        else:
            # (ext) The URL is from an external page
            ext_counter += 1
            if row.article_code!='':
                appendable_url_dict=melty_article_API_request(row.article_code,test)
            appendable_url_dict['id']='EXT'+str(ext_counter)
            appendable_url_dict['url']=row.url
            appendable_url_dict['category']=ext_url_classificator(row.url)
            ext=ext.append(appendable_url_dict,ignore_index=True)
    print("     %d of %d URLs inspected (%.2f%%) in %.0f seconds."%(total_loops, total_loops, 100.0,timelib.time()-start_time))
    return mar, mna, ext;

##################################
##################################
####                          ####
#### MAIN PROGRAM             ####
####                          ####
##################################
##################################

def create_new_database(new_urls,test=False):

    ##################################
    # DIVIDE THE DB IN 3 PARTS
    mar=pd.DataFrame(columns=['id', 'url', 'category', 'topic', 'melty_page_type', 'article_API_consultation',
                              'page_scrap_consultation', 'melty_thema_name'],dtype='object')
    mna=pd.DataFrame(columns=['id', 'url', 'category', 'topic', 'melty_page_type', 'article_API_consultation',
                              'page_scrap_consultation', 'melty_thema_name'],dtype='object')
    ext=pd.DataFrame(columns=['id', 'url', 'category', 'topic', 'melty_page_type',' article_API_consultation',
                              'page_scrap_consultation', 'melty_thema_name'],dtype='object')

    #############################################
    # RETRIEVING THE INFORMATION FOR THE NEW URLs
    new_mar, new_mna, new_ext = tables_updater(new_urls,mar,mna,ext,test)
    output_urls=pd.concat([new_mar,new_mna,new_ext],axis=0)
    output_urls.fillna('',inplace=True)
    output_urls.reset_index(inplace=True,drop=True)
    
    #############################################
    # TRANSLATING AND FORMATTING FRENCH THEMA 
    output_urls['topic'] = output_urls.melty_thema_name.apply(lambda x: thema_mapper(x))
    
    #############################################
    # SELECTING USEFUL ENTRIES
    output_urls = output_urls[['id','url','category','topic']]

    return output_urls;

