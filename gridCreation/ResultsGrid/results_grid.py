import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import ConfigParser
from dataStorage import upload_to_Elasticsearch
from itertools import izip, product
import datetime as dt

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

def create_combinations(**kwargs):
    #creates a list of dicts with all combinations of combined kwarg lists
    return [dict(izip(kwargs,x)) for x in (product(*kwargs.itervalues()))]

def create_results_grid():
    #input: index name, document type, dict of fields and lists of field values by which to create the grid
    #output: creates a grid index in ES with an entry for each combination of kwargs elements
    start = dt.datetime.now()
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    #get list of zip code tabulation areas
    zctas = [result['_id'] for result in helpers.scan(es,index='nyc_zip_codes',doc_type='zip_codes')]

    #define the start and end of a leap year for collecting all month-day combos
    start_date= dt.datetime(2016,1,1,0,0)
    end_date = dt.datetime(2017,1,1,0,0)

    t_delta = dt.timedelta(days=1) #one day
    next_date = start_date
    dates = []
    while next_date < end_date:
        dates.append(dt.datetime.strftime(next_date,'%m-%d'))
        next_date+=t_delta
        
    
    docs = create_combinations(Hour=range(0,24),DayOfWeek=range(1,8),MonthDay=dates,Zipcode=zctas)

    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,index="prediction_results",doc_type="results_grid")

    print "Done creating results grid. Took %s" % str(dt.datetime.now() - start)
    
