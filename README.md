# reader-embedding

Working with embeddings for reading sessions.


## code/

**Generating reading sessions**
- reading-sessions_01-get-data-from-webrequest.ipynb  
  get reading sessions from from webrequest
- reading-sessions_02-filter-sessionize.ipynb  
  filter and sessionize reading sessions for different projects (wikidata, enwiki, etc.)
- reading-sessions_03-make-train-dev-test.ipynb  
  split data (or subsample) into train-dev-test data

**Running word2vec on the filtered sessions**
- run-word2vec.ipynb    
  runnning fasttetext's word2vec and save model

**Creating lists of related articles based on embedding**
- list-of-related-articles_from-word2vec.ipynb  
  create list of related articles based on word2vec embedding from reading sessions

**Running morelike for evaluating next-article-prediction**
- run-morelike.ipynb  
  notebook to show basic functionality
- morelike.py  
  script to get eval-metrics for morelike on testfile

Note that some of the notebooks can only be run on Wikimedia's stat-machines.
