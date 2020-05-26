
import argparse
import datetime
import numpy as np
import os,sys
import json
import time
import random
import requests




def main():
    parser = argparse.ArgumentParser(
        description='Evaluate morelike in the task of next-article-prediction.\
        Try: python morelike.py -i <file> -k 100 -n 1000 -w enwiki'
        )
    parser.add_argument("-i","--input_file",
                        default="../output/reading-sessions-corpora/enwiki_sample-100000.test",
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


    args = parser.parse_args()


    try:
        queries = prepare_queries_pairs(args.input_file,N_max=args.N_eval_max)
    except:
        print('Could not load sessions from input file %s'%(args.input_file))
        print('Check that the file exists')
        return

    result = queriesPairsEval(queries,wiki=args.wiki,k=args.k)
    with open(args.output_results,'w') as fout:
        fout.write(json.dumps(result) + '\n')
    print('Done: results are in %s'%args.output_results)

def prepare_queries_pairs(f, N_max = -1 ):
    '''
    from a file containing sequences of pageview.
    select one random pair of consecutive pageivews.
    returns a list of tuples [(src,trg)], where src, trg are of type str.

    get at most N_max pairs (default is -1 == all).
    '''
    queries = []; count=0
    for line in open(f):
        session = line.strip().split(" ")
        if len(session)>=2:
            idx_src = random.randint(0,len(session)-2)
            queries.append(( session[idx_src],session[idx_src+1] ))
            count+=1
        if count == N_max:
            break
    print("Extracted "+str(count)+" pairs")
    return queries

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
        'srwhat': 'text',
        'srprop': 'wordcount',
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

def queriesPairsToRank(queries,wiki,k=100):
    '''
    from a list of pairs (src,target)
    - get the k nearest neighbors of src via morelike in specific wiki
    - check rank of trg among nearest neighbors
    '''
    t_rest = 0.1 ## be nice to morelike API
    rank_list = []
    for pid_src,pid_trg in queries:
        result = morelikeFromPageid(pid_src,wiki)
        pid_src_nn = [str(nn['pageid']) for nn in result  ]
        try:
            rank = pid_src_nn.index(pid_trg)+1
        except ValueError:
            rank = 1e6
        rank_list.append(rank)
        
        time.sleep(t_rest)
    return np.array(rank_list)

 
def metrics(mrr_list):
    '''
    calculate metrics associated with rank querying from a list of ranks
    - mrr (mean reciprocal rank)
    - recall@k, whether trg was among top-k in mrr-list
    '''
    mrr = np.mean(1/mrr_list)
    recall1 = np.where((mrr_list <= 1) & (mrr_list != 1e6))[0].shape[0]/mrr_list.shape[0]
    recall10 = np.where((mrr_list <= 10) & (mrr_list != 1e6))[0].shape[0]/mrr_list.shape[0]
    recall50 = np.where((mrr_list <= 50) & (mrr_list != 1e6))[0].shape[0]/mrr_list.shape[0]
    recall100 = np.where((mrr_list <= 100) & (mrr_list != 1e6))[0].shape[0]/mrr_list.shape[0]
    
    dict_result = {
        'N':mrr_list.shape[0], 
        'MRR':mrr,
        'Recall@1':recall1,
        'Recall@10':recall10,
        'Recall@50':recall50,
        'Recall@100':recall100
    }
    return dict_result
#     return mrr_list.shape[0], mrr, recall1, recall10, recall50, recall100

def queriesPairsEval(queries,wiki,k=100):
    list_rank = queriesPairsToRank(queries,wiki,k=k)
    return metrics(list_rank)


if __name__ == "__main__":
    main()