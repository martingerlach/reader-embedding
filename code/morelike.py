
import argparse
import datetime
import numpy as np
import os,sys
import json
import time
import random
import requests

from utils.nextarticle import prepare_queries
from utils.metrics import ranks_metrics



def main():
    parser = argparse.ArgumentParser(
        description='Evaluate morelike in the task of next-article-prediction.\
        Try: python morelike.py -i <file> -k 100 -n 1000 -w enwiki'
        )
    parser.add_argument("-i","--input_file",
                        default="../output/reading-sessions-corpora/enwiki/enwiki_sample-100000.test",
                        help="File with reading sessions")

    parser.add_argument("-w","--wiki",
                        default="enwiki",
                        help="Wiki in which to make the morelike query (e.g. enwiki)")

    parser.add_argument("-n","--N_eval_max",
                        default=100,
                        type = int,
                        help="Maximum number of queries from sessions in ftest")
    
    parser.add_argument("-k","--k",
                        default=100,
                        type = int,
                        help="Number of nearest neighbors to query")

    parser.add_argument("-o","--output_results",
                        default="example_output_data.txt",
                        help="Output json with different metrics")

    parser.add_argument("-s","--seed",
                        default=None,
                        help="seed for random selection of ")

    parser.add_argument("-r","--rest",
                        default=0.1,
                        type = float,
                        help="time to rest between calls to the morelike API ")

    args = parser.parse_args()

    ## make query pairs: list of source-target pairs of articles from reading sessions
    try:
        queries = prepare_queries(args.input_file,N_max=args.N_eval_max,seed=args.seed)
    except:
        print('Could not load sessions from input file %s'%(args.input_file))
        print('Check that the file exists')
        return

    ## assign ranks to the queries via morelike
    ranks = queriesRanks(queries,wiki=args.wiki,k=args.k, rest = args.rest)

    ## calculate some metrics from the ranks
    result = ranks_metrics(ranks)
    ## write to file
    with open(args.output_results,'w') as fout:
        fout.write(json.dumps(result) + '\n')
    print('Done: results are in %s'%args.output_results)

def titleFromPageid(page_id,wiki):
    '''
    query wikipedia-API to get the pagetitle from a pageid
    '''
    ## get the page-ids
    api_url_base = 'https://%s.wikipedia.org/w/api.php'%( wiki.replace('wiki','') )
    params = {
        "action": "query",
        "pageids": page_id,
        "prop": "pageprops",
        "format": "json",
    }
    try:
        response = requests.get( api_url_base,params=params).json()
        if 'query' in response:
            if 'pages' in response['query']:
                title = response['query']['pages'].get(page_id,{}).get('title','')

    except:
        title = ''
    return title

## morelike search
def morelikeFromTitle(title,wiki,k=100):
    '''
    do morelike search https://www.mediawiki.org/wiki/Help:CirrusSearch#Morelike
    get k recommendations for a page-title in a given wiki.
    Return titles and pageids.
    '''

    api_url_base = 'https://%s.wikipedia.org/w/api.php'%( wiki.replace('wiki','') )
    ## https://www.mediawiki.org/wiki/API:Search
    ## https://www.mediawiki.org/wiki/Help:CirrusSearch#Morelike

    params = {
        'action': 'query',
        'list': 'search',
        'format': 'json',
        'srsearch': 'morelike:'+title,
        'srnamespace' : 0,
        # 'srwhat': 'text',
        'srqiprofile': 'classic_noboostlinks',
        'srprop': 'pageid',
        'srlimit': k
    }
    try:
        response = requests.get( api_url_base,params=params).json()
    except:
        print('Could not do morelike search for %s in %s. Try another article or another language.' % (title,wiki))
        return [] 

    if 'query' not in response or 'search' not in response['query']:
        print('Could not do morelike search for %s in %s. Try another article or another language.' % (title,wiki))
        return []
    return response['query']['search']

def morelikeFromPageid(page_id,wiki,k=100):
    '''
    before querying morelikeFromTitle we have to get the title from the pageid
    '''
    title = titleFromPageid(str(page_id),wiki)
    if len(title)>0:
        result = morelikeFromTitle(title,wiki,k=k)
    else:
        result = []
    return result

def queriesRanks(queries,wiki,k=100, rest = 0.1):
    '''
    from a list of pairs (src,target)
    - get the k nearest neighbors of src via morelike in specific wiki
    - check rank of trg among nearest neighbors
    '''
    rank_list = []
    for pid_src,pid_trg in queries:
        result = morelikeFromPageid(pid_src,wiki)
        pid_src_nn = [str(nn['pageid']) for nn in result  ]
        try:
            rank = pid_src_nn.index(pid_trg)+1
        except ValueError:
            rank = 1e6
        rank_list.append(rank)
        
        time.sleep(rest) ## be nice to the AOU
    return np.array(rank_list)

if __name__ == "__main__":
    main()