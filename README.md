# reader-embedding

Constructing embedding of reading sessions.

**notebooks/**


* Extracting data from webrequest logs  
 * 01a_reading-sessions_get-data-from-webrequest_all-wikis.ipynb   
   - for all wikipedias mathcing qids.
 * 01b_reading-sessions_get-data-from-webrequest_single-wiki.ipynb   
   - for a single wikipedias only keeping page-ids.


* Sessionizing and filtering  
 * 02a_reading-sessions_filter-sessionize_qid.ipynb  
   - make sessions of qids (all wikis or single wiki)
 * 02b_reading-sessions_filter-sessionize_pageid.ipynb
   - make sessions of pageids (only single wikis)


* Running word2vec on the filtered sessions  
 * 03_reading-sessions_run-word2vec


Note that some of the notebooks can only be run on Wikimedia's stat-machines.
