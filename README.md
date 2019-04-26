# LogPreprocessor
Log preprocessor for log files provided by Melty for the ALGODIV project.
Melty Log Preprocessor for ALGODIV
----------------------------------

Pedro Ramaciotti Morales
LIP 6-UMPC, Sorbonne University, Paris
ALGODIV ANR-15-CE38-001

This package includes two programs, one for preprocessing log files provided by
Melty and a different one to build simple CSV dabatases of pages with useful data
such as the category (e.g. article, forum, video stream) and thema (e.g. series,
TV, sports) of each page.


Input Log Files
---------------

Input files are CSV files. Each line corresponds to a request made to the server,
and the format is similar to NCSA log format, but with some changes. The fields
of each line are:

user ID  
timestamp  
timezone  
request Command (GET+URL+Protocol)  
response (Response sent from server)  
size (Size of the response)  
referrer (The page where the resquest originated)  
agent (Software used to make the request and other client-side information)  


Log Preprocessor (log_preprocessor.py)
--------------------------------------

Use:

"python3 log_preprocessor.py -il \<input-log-file\> -ol <\output-preprocessed-log\> \<output-preprocessed-pages\>".

The program will eliminate requests made by self-delcared robots (looking for
keywords in agent field present in file robot_agent_stringmatch.txt) and requests
made by users with suspicious unhuman behavior. The output file is a CSV file
where each line is a request, whose fields are:

userID  
timestamp  
requested_pageID  
referrer_pageID  
device (Phone, Computer, Tablet or Other)  

The program also returns a CSV file contaning all the pages that appear in the
preprocessed log (be it as requested or referrer). Each line is a page; whose fields are:

pageID  
category (topic page, quiz...)  
topic (Series, Movies...)  
external (from Melty or not)  
folderID  


To Do (beware of):
------------------

* Devise a way to further identify differences inside some popular themas (TV, series).
* There is a division by zero in the countdown display when files contain too few requests.
* Very few pages have unexpected HTML format, which results in melty_thema_name='folder_id//',
  their melty_thema_name is in reality 'null' in HTML.
* Empty melty_thema_name (i.e. '') are the results of unsuccessful API/crawling consultations
  but also not performed consultations for some EXT (external, non-sister article page).
* Crawling/scrapping attempt could be implemented for external pages from sister site that
  do not have an article ID.
* Considering cropping some URLs' parts, merging nodes. E.g.
  r.search.yahoo.com/_ylt=A2KLqIpmeK5Z.QMAE04M24lQ -> r.yahoo.com. This would speed up
  some processes.
* Some folder pages have a different URL format, ending in id_world_index/folderID.
This are not treated yet by the program.
