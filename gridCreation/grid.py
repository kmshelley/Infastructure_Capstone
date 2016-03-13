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

zip_codes = config.get('zip codes','zip_codes')

def create_combinations(**kwargs):
    #creates a list of dicts with all combinations of combined kwarg lists
    return [dict(izip(kwargs,x)) for x in (product(*kwargs.itervalues()))]

def combination_iter(**kwargs):
    #creates an iterator of dicts with all combinations of combined kwarg lists
    for x in product(*kwargs.itervalues()):
        yield dict(izip(kwargs,x))

def feature_grid(**kwargs):
    grid_iter = combination_iter(kwargs)
    try:
        g = grid_iter.next()

        #datetime fields
        g['grid_month'] = g['grid_fullDate'].month
        g['grid_day'] = g['grid_fullDate'].day
        g['grid_dayOfWeek'] = g['grid_fullDate'].isoweekday()
        g['grid_hourOfDay'] = g['grid_fullDate'].hour
        g['grid_fullDate'] = dt.datetime.strftime(g['grid_fullDate'],'%Y-%m-%dT%H:%M%S')

        #collision fields
        g['grid_collision_counter'] = 0
        g['grid_isAccident'] = 0
        
        
        

    except StopIteration:
        pass 
    

       
def create_full_feature_grid(index="dataframe",doc_type="rows"):
    #input: index name, document type
    #output: creates a grid index in ES with an entry for each combination of kwargs elements
    start = dt.datetime.now()
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    first_coll = upload_to_Elasticsearch.get_first_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
    first_coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record
    rnd_start = dt.datetime(first_coll_time.year,first_coll_time.month,first_coll_time.day,first_coll_time.hour,0,0)

    last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
    last_coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record
    rnd_stop = dt.datetime(last_coll_time.year,last_coll_time.month,last_coll_time.day,last_coll_time.hour,0,0)
    
    t_delta = dt.timedelta(seconds=3600) #one hour
    next_time = rnd_start
    times = []
    while next_time <= rnd_stop:
        times.append(next_date)
        next_date+=t_delta
        
    grid = create_combinations(grid_fullDate=times,grid_zipcode=zip_codes)
    
    '''doc = {grid_collision_counter: {
            type: "long"
            },
            grid_day: {
            type: "long"
            },
            grid_dayOfWeek: {
            type: "long"
            },
            grid_fullDate: {
            type: "date",
            format: "strict_date_optional_time||epoch_millis"
            },
            grid_hourOfDay: {
            type: "long"
            },
            grid_id: {
            type: "string"
            },
            grid_isAccident: {
            type: "long"
            },
            grid_month: {
            type: "long"
            },
            grid_zipcode: {
            type: "long"
            },
            weather_Fog: {
            type: "long"
            },
            weather_HourlyPrecip: {
            type: "string"
            },
            weather_Rain: {
            type: "long"
            },
            weather_SkyCondition: {
            type: "string"
            },
            weather_SnowHailIce: {
            type: "long"
            },
            weather_Visibility: {
            type: "double"
            },
            weather_WeatherType: {
            type: "string"
            },
            weather_WetBulbFarenheit: {
            type: "double"
            },
            weather_WindSpeed: {
            type: "double"
            }
            }'''
    
    docs = create_combinations(Hour=range(0,24),DayOfWeek=range(1,8),MonthDay=dates,Zipcode=zip_codes)

    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,index=index,doc_type=doc_type)

    print "Done creating results grid. Took %s" % str(dt.datetime.now() - start)

    
def create_results_grid(index="prediction_results",doc_type="results_grid"):
    #input: index name and doc_type
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
        
    
    docs = create_combinations(Hour=range(0,24),DayOfWeek=range(1,8),MonthDay=dates,Zipcode=zip_codes)

    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(docs,index=index,doc_type=doc_type)

    print "Done creating results grid. Took %s" % str(dt.datetime.now() - start)
    
combo = create_combinations(hours=range(0,24),days=range(1,8))
print type(combo[0])
