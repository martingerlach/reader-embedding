import os, sys
import datetime
import calendar
import time
import string
import random
import argparse
from pyspark.sql import functions as F, types as T, Window, SparkSession


'''
process webrequest table to get reading sessions
- returns filename where reading sessions are stored locally
    - ../output/graph/graph_<WIKI>_<SNAPSHOT>_nodes.parquet
    - ../output/graph/graph_<WIKI>_<SNAPSHOT>_edges.parquet

- USAGE:
PYSPARK_PYTHON=python3.7 PYSPARK_DRIVER_PYTHON=python3.7 spark2-submit --master yarn --executor-memory 8G --executor-cores 4 --driver-memory 2G  generate_data-graph.py -l simple
- optional
    - s,snapshot YYYY-MM     
'''

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang","-l",
                        default="enwiki",
                        type = str,
                        help="language to parse (en or enwiki)")
    
    parser.add_argument("--snapshot","-s",
                        default=None,
                        type = str,
                        help="month of snapshot (YYYY-MM); default: current month - 2months")


    args = parser.parse_args()
    lang = args.lang.replace('wiki','')
    wiki = lang+'wiki'
    snapshot = args.snapshot

    if snapshot!=None:
        try:
            date_snapshot = datetime.datetime.strptime(t1,'%Y-%m')
        except ValueError:
            print('Provide correct day-format YYYY-MM')
    else:
        date_snapshot = datetime.date.today()-datetime.timedelta(days=60)

    snapshot = date_snapshot.strftime('%Y-%m')




    PATH_out_hdfs = '/tmp/reader-embedding/graph/'
    PATH_out_local = os.path.abspath('../output/graph/')

    ### start
    spark = SparkSession.builder\
        .master('yarn')\
        .appName('get-network')\
        .enableHiveSupport()\
        .getOrCreate()

    ## all pages in the main namespace (incl redirects)
    # page_id, page_title, page_is_redirect
    df_pages = (
        ## select table
        spark.read.table('wmf_raw.mediawiki_page')
        ## select wiki project
        .where( F.col('wiki_db') == wiki )
        .where( F.col('snapshot') == snapshot )
        ## main namespace
        .where(F.col('page_namespace') == 0 )
        .select(
            'page_id',
            'page_title',
            'page_is_redirect'
        )
    )



    ## redirects table with page_ids from/to
    ## we join the pages table to get page_id for the redirected-to page
    df_redirect = (
        ## select table
        spark.read.table('wmf_raw.mediawiki_redirect')
        ## select wiki project
        .where( F.col('wiki_db') == wiki )
        .where( F.col('snapshot') == snapshot )
        .where(F.col('rd_namespace') == 0 )
        .select(
            F.col('rd_from').alias('page_id_from'),
            F.col('rd_title').alias('page_title')
        )
        
        ## get the page-ids for the redirected-to pages
        .join(df_pages,on='page_title',how='inner')
        
        ## select only page-ids
        .select(
            F.col('page_id_from').alias('rd_from'),
            F.col('page_id').alias('rd_to')
        )
    )

    ## get the pagelinks table with page_id_from and page_id_to
    ## only keep links starting from non-redirect pages
    ## join pages-table to get page-ids for redirect-to pages
    df_links = (
        ## select table
        spark.read.table('wmf_raw.mediawiki_pagelinks')
        ## select wiki project
        .where( F.col('wiki_db') == wiki )
        .where( F.col('snapshot') == snapshot )
        
        ## namespace of source and target page
        .where(F.col('pl_from_namespace') == 0 )
        .where(F.col('pl_namespace') == 0 )
        
        .withColumnRenamed('pl_from','page_id_from')
        .withColumnRenamed('pl_title','page_title')
        
        ## only keep links that originate from a page that is not a redirect 
        ## by joining the pages-table with the non-redirect pages
        .join(
            df_redirect.withColumnRenamed('rd_from','page_id_from'),
            on = 'page_id_from',
            how = 'left_anti'
        )
        ## map page_title_to page_id_to by joining the pages-df
        .join(
            df_pages,
            on='page_title',
            how='inner'
        )
        .withColumnRenamed('page_id','page_id_to')
        .select('page_id_from','page_id_to')
    )

    ## resolve the redirects in the links-table by joining the redirect table
    df_links_resolved = (
        df_links
        ## join in the redirects
        .join(
            df_redirect,
            df_links['page_id_to'] == df_redirect['rd_from'],
            how = 'left'
        )
        ## select the redirected link (otherwise keep the old)
        .withColumn('page_id_to_resolved', F.coalesce(F.col('rd_to'),F.col('page_id_to')) )
        .select(
            F.col('page_id_from').alias('page_id_from'),
            F.col('page_id_to_resolved').alias('page_id_to')
        )
        ## remove duplicate links
        .distinct()
        .select(
            'page_id_from',
            'page_id_to'
        )
    #     .orderBy('page_id_from','page_id_to')
    )

    ## join the wikidata-item to each pageview
    ## we keep only pageviews for which we have a correpsionding wikidata-item id

    ## table with mapping wikidata-ids to page-ids
    ## partition wikidb and page-id ordered by snapshot
    w_wd = Window.partitionBy(F.col('wiki_db'),F.col('page_id')).orderBy(F.col('snapshot').desc())
    df_wd = (
        spark.read.table('wmf.wikidata_item_page_link')
        ## snapshot: this is a partition!
        .where(F.col('snapshot') >= '2020-07-01') ## resolve issues with non-mathcing wikidata-items
        ## only wikis (enwiki, ... not: wikisource)
        .where(F.col('wiki_db')==wiki)
        .withColumn('item_id_latest',F.first(F.col('item_id')).over(w_wd))
        .select(
            'page_id',
            F.col('item_id_latest').alias('item_id')
        )
        .drop_duplicates()
    )

    ## get the final nodes table
    df_from = df_links_resolved.select('page_id_from').distinct().withColumnRenamed('page_id_from','page_id')
    df_to = df_links_resolved.select('page_id_to').distinct().withColumnRenamed('page_id_to','page_id')
    df_nodes_sel = df_from.join(df_to,on='page_id',how='outer')

    # all nodes from the pages-table which appear in the links_resolved-table (from/to)
    df_nodes = (
        df_pages
        .join(
            df_nodes_sel,
            on = 'page_id',
            how = 'left_semi'
        )
        .join(df_wd,on='page_id',how='left')
        .select(
            'page_id',
            'page_title',
            'item_id'
        )
    )

    ## saving the files as parquet and copy to local
    FNAME_out = 'graph_%s_%s_edges.parquet'%(wiki,snapshot)

    FILE_out_hdfs = os.path.join(PATH_out_hdfs,FNAME_out)
    df_links_resolved.write.mode('overwrite').parquet(path=FILE_out_hdfs)
    os.system("hadoop fs -get %s %s"%(FILE_out_hdfs,PATH_out_local))
    os.system("hadoop dfs -rm -r %s"%(FILE_out_hdfs))

    FNAME_out = 'graph_%s_%s_nodes.parquet'%(wiki,snapshot)

    FILE_out_hdfs = os.path.join(PATH_out_hdfs,FNAME_out)
    df_nodes.write.mode('overwrite').parquet(path=FILE_out_hdfs)
    os.system("hadoop fs -get %s %s"%(FILE_out_hdfs,PATH_out_local))
    os.system("hadoop dfs -rm -r %s"%(FILE_out_hdfs))




if __name__ == "__main__":
    main()

