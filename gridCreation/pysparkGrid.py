
# coding: utf-8

# # PySpark Grid Creation and Feature Engineering
# 
# Running the cells in this notebook will create or update the collisions data grid, including machine learning features, of NYC zip codes and time stamps stored in an Elasticsearch index.

# In[1]:

#Python libraries
from pyspark import SparkContext, SparkConf

'''' run with:
$SPARK_HOME/bin/spark-submit \
  --master spark://spark1:7077 \
  --executor-memory 14G \
  --conf spark.default.parallelism=36 \
  --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.2.0.jar \ 
  ./gridCreation/pysparkGrid.py 
'''
conf = SparkConf().setAppName('GridCreation')
sc = SparkContext(conf=conf)


import sys
sys.path.append('/root/Infrastructure_Capstone')
import os
import subprocess
import math
from shapely.geometry import Polygon
from pyproj import Proj
from pprint import pprint
from copy import deepcopy
import datetime as dt
from dateutil.parser import parse
import json, csv
from itertools import izip, product

from dataStorage import upload_to_Elasticsearch

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import indices

import ConfigParser

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_hosts = config.get('ElasticSearch','hostlist')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

zip_codes = config.get('zip codes','zip_codes').split(',')
broadcast_zip = sc.broadcast(zip_codes)

central_park_wban = "94728"

# ### Grid Creation Functions
# 
# The following cells will create the grid from scratch.

# In[2]:

def create_combinations(**kwargs):
    #creates a list of dicts with all combinations of combined kwarg lists
    return [dict(izip(kwargs,x)) for x in (product(*kwargs.itervalues()))]

def combination_iter(**kwargs):
    #creates an iterator of dicts with all combinations of combined kwarg lists
    for x in product(*kwargs.itervalues()):
        yield dict(izip(kwargs,x))


def feature_grid(timestamp):
    #create grid record
    #datetime fields
    output = []
    for zipcode in broadcast_zip.value:
        g = {}
        g['grid_zipcode'] = zipcode
        g['grid_id'] = dt.datetime.strftime(timestamp,'%Y%m%d%H%M') + '_' + zipcode
        
        g['grid_year'] = timestamp.year
        g['grid_month'] = timestamp.month
        g['grid_day'] = timestamp.day
        g['grid_dayOfWeek'] = timestamp.isoweekday()
        g['grid_Weekday'] = 1 if g['grid_dayOfWeek'] < 6 else 0
        g['grid_hourOfDay'] = timestamp.hour
        g['grid_Morning'] = 1 if g['grid_hourOfDay'] in range(6,10) else 0
        g['grid_Midday'] = 1 if g['grid_hourOfDay'] in range(10,14) else 0
        g['grid_Afternoon'] = 1 if g['grid_hourOfDay'] in range(14,18) else 0
        g['grid_Evening'] = 1 if g['grid_hourOfDay'] in range(18,22) else 0
        g['grid_Midnight'] = 1 if (g['grid_hourOfDay'] >= 22 or g['grid_hourOfDay'] < 6) else 0
        g['grid_fullDate'] = dt.datetime.strftime(timestamp,'%Y-%m-%dT%H:%M:%S')
        
        
        #add collisions and weather data
        g = add_collisions(g)
        g = add_weather(g)
    
        ### ADD ADDITIONAL FEATURE ENGINEERING FUNCTIONS HERE ###
        output.append((g['grid_id'],g))
    
    return output
    


# ### Data Cleaning Functions
# The following cells contain functions to collect and clean disparate data sets and add them to the grid. These functions can be used with Spark transformations and actions.
# 
# Add new functions for data cleaning and feature engineering here.

# #### Collisions

# In[5]:

def add_collisions(g):
    #Adds total collision counts and isAccident boolean fields
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    hour = parse(g['grid_fullDate']).replace(tzinfo=None)
    query = '''{
                "query": {
                    "bool": {
                        "must" : { "term": { "collision_ZCTA_ZIP_NoSuffix" : "%s"} },
                        "must" : {
                            "range" : {
                                "collision_DATETIME_C" : {
                                        "gte": "%s",
                                        "lt": "%s",
                                        "format": "MM/dd/yyyy HH:mm"
                                }
                            }
                        } 
                    }
                }
            }''' % (g['grid_zipcode'],dt.datetime.strftime(hour,'%m/%d/%Y %H:%M'),dt.datetime.strftime(hour + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M'))
    collisions = list(helpers.scan(es,query=query,index='saferoad',doc_type='collisions')) #get the collisions for that zip code and hour
    
    g['grid_collision_counter'] = len(collisions)
    g['grid_isAccident'] = 1 if g['grid_collision_counter'] > 0 else 0

    #fatalities
    g['grid_totalFatalities'] = sum([float(rec['_source']['collision_NUMBER OF PERSONS KILLED']) for rec in collisions])
    g['grid_motoristFatalities'] = sum([float(rec['_source']['collision_NUMBER OF MOTORIST KILLED']) > 0 for rec in collisions])
    g['grid_cyclistFatalities'] = sum([float(rec['_source']['collision_NUMBER OF CYCLIST KILLED']) for rec in collisions])
    g['grid_pedestrianFatalities'] = sum([float(rec['_source']['collision_NUMBER OF PEDESTRIANS KILLED']) for rec in collisions])

    #injuries
    g['grid_totalInjuries'] = sum([float(rec['_source']['collision_NUMBER OF PERSONS INJURED']) for rec in collisions])
    g['grid_motoristInjuries'] = sum([float(rec['_source']['collision_NUMBER OF MOTORIST INJURED']) for rec in collisions])
    g['grid_cyclistInjuries'] = sum([float(rec['_source']['collision_NUMBER OF CYCLIST INJURED']) for rec in collisions])
    g['grid_pedestrianInjuries'] = sum([float(rec['_source']['collision_NUMBER OF PEDESTRIANS INJURED']) for rec in collisions])

    #binary injury/fatality variable
    g['grid_anyFatality'] = 1 if g['grid_totalFatalities'] > 0 else 0
    g['grid_anyInjury'] = 1 if g['grid_totalInjuries'] > 0 else 0
    
    
    return g
    


# #### Weather

#functions to add weather data to grid based on closest weather station. If closest station does not have a reading 
#for that time period, use Central Park station
def get_weather_reading(wban,time):
    #searches ES for weather readings from station id wban, closest to the given time
    #find weather observations for the nearest weather station and time
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    

    query = '''{
                "query": {
                    "bool": {
                        "must" : { "term": { "weather_WBAN" : "%s"} },
                        "must" : {
                            "range" : {
                                "weather_DateTime" : {
                                        "gte": "%s",
                                        "lt": "%s",
                                        "format": "MM/dd/yyyy HH:mm"
                                }
                            }
                        } 
                    }
                }
            }''' % (wban,dt.datetime.strftime(time + dt.timedelta(seconds=-3600),'%m/%d/%Y %H:%M'),dt.datetime.strftime(time + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M'))

    
    observations = list(helpers.scan(es,query=query,index='weather',doc_type='hourly_obs')) #get the first observation returned
    if len(observations) > 0:
        min_diff = float('inf')
        best_obs = None
        #iterate through the returned observations, add the closest observation in time
        for obs in observations:
            obs_time = parse(obs['_source']['weather_DateTime']).replace(tzinfo=None)
            if abs((obs_time - time).total_seconds()) < min_diff:
                best_obs = obs['_source']
                min_diff = abs((obs_time - time).total_seconds())
        return best_obs
    else:
        return None

def add_weather(g):
    #adds weather data to grid
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    proj = Proj(init='epsg:2263')
    updates = []
    idx=0
    update = deepcopy(g)
    date_hour = parse(g['grid_fullDate']).replace(tzinfo=None)
    
    query = '{               "query": {                 "bool": {                   "must": {                     "wildcard": { "zipcode" : "%s*" }                   }                 }               }             }' % g['grid_zipcode']

    #find the largest zip code area to represent the grid area
    max_area = 0
    wban = None
    #query the zip codes, finding all zip shapes that contain the current colision
    for shape in helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'):
        coords = [proj(lng,lat) for lng,lat in shape['_source']['coords']['coordinates'][0]]
        poly = Polygon(coords)
        if poly.area > max_area:
            #get the largest zip code by geographic area
            max_area = poly.area
            wban = shape['_source']['closest_wban']

    #find weather observations for the nearest weather station and time
    observation = get_weather_reading(wban,date_hour)
    if observation:
        #numerical fields, change 99999 to avg values        
        if float(observation['weather_WetBulbFarenheit']) <> 99999:
            update['weather_WetBulbFarenheit'] = observation['weather_WetBulbFarenheit'] 
        else:
            update['weather_WetBulbFarenheit'] = 70
            
        if float(observation['weather_HourlyPrecip']) <> 99999:
            update['weather_HourlyPrecip'] = observation['weather_HourlyPrecip']
        else:
            update['weather_HourlyPrecip'] = 0
    
        if float(observation['weather_WindSpeed']) <> 99999:
            update['weather_WindSpeed'] = observation['weather_WindSpeed']
        else:
            update['weather_WindSpeed'] = 0
            
        if float(observation['weather_Visibility']) <> 99999:
            update['weather_Visibility'] = observation['weather_Visibility']
        else:
            update['weather_Visibility'] = 10        

        #string fields

        update['weather_SkyCondition'] = observation['weather_SkyCondition'] #Adjust this for just condition?
        update['weather_WeatherType'] = observation['weather_WeatherType']

        weather_list = observation['weather_WeatherType'].split(' ') #space delimited list
        #Types of rain
        update['weather_Rain'] = 0 #no rain
        if '-RA' in weather_list or '-DZ' in weather_list or '-SH' in weather_list or '-FZRA' in weather_list: 
            update['weather_Rain'] = 1 #light rain
        if 'RA' in weather_list or 'DZ' in weather_list or 'SH' in weather_list or 'FZRA' in weather_list: 
            update['weather_Rain'] = 2 #moderate rain
        if '+RA' in weather_list or '+DZ' in weather_list or '+SH' in weather_list or '+FZRA' in weather_list: 
            update['weather_Rain'] = 3 #heavy rain


        #Types of snow/hail/ice
        update['weather_SnowHailIce'] = 0 #none
        if '-SN' in weather_list or '+SG' in weather_list or '-GS' in weather_list or '-GR' in weather_list or '-PL' in weather_list or '-IC' in weather_list:
            update['weather_SnowHailIce'] = 1 #light
        if 'SN' in weather_list or '+SG' in weather_list or 'GS' in weather_list or 'GR' in weather_list or 'PL' in weather_list or 'IC' in weather_list:
            update['weather_SnowHailIce'] = 2 #moderate 
        if '+SN' in weather_list or '+SG' in weather_list or '+GS' in weather_list or '+GR' in weather_list or '+PL' in weather_list or '+IC' in weather_list:
            update['weather_SnowHailIce'] = 3 #heavy             


        #Types of fog/mist
        update['weather_Fog'] = 0 #none
        if '-FG' in weather_list or '-BR' in weather_list or '-HZ' in weather_list:
            update['weather_Fog'] = 1 #light
        if 'FG' in weather_list or 'BR' in weather_list or 'HZ' in weather_list:
            update['weather_Fog'] = 2 #moderate 
        if '+FG' in weather_list or '+BR' in weather_list or '+HZ' in weather_list or 'FG+' in weather_list:
            update['weather_Fog'] = 3 #heavy 

    else:
        #find weather observations for central park station and time
        observation = get_weather_reading(central_park_wban,date_hour)
        if observation:
            #numerical fields, change 99999 to avg values        
            if float(observation['weather_WetBulbFarenheit']) <> 99999:
                update['weather_WetBulbFarenheit'] = observation['weather_WetBulbFarenheit'] 
            else:
                update['weather_WetBulbFarenheit'] = 70
                
            if float(observation['weather_HourlyPrecip']) <> 99999:
                update['weather_HourlyPrecip'] = observation['weather_HourlyPrecip']
            else:
                update['weather_HourlyPrecip'] = 0
        
            if float(observation['weather_WindSpeed']) <> 99999:
                update['weather_WindSpeed'] = observation['weather_WindSpeed']
            else:
                update['weather_WindSpeed'] = 0
                
            if float(observation['weather_Visibility']) <> 99999:
                update['weather_Visibility'] = observation['weather_Visibility']
            else:
                update['weather_Visibility'] = 10      

            #string fields

            update['weather_SkyCondition'] = observation['weather_SkyCondition'] #Adjust this for just condition?
            update['weather_WeatherType'] = observation['weather_WeatherType']

            weather_list = observation['weather_WeatherType'].split(' ') #space delimited list
            #Types of rain
            update['weather_Rain'] = 0 #no rain
            if '-RA' in weather_list or '-DZ' in weather_list or '-SH' in weather_list or '-FZRA' in weather_list: 
                update['weather_Rain'] = 1 #light rain
            if 'RA' in weather_list or 'DZ' in weather_list or 'SH' in weather_list or 'FZRA' in weather_list: 
                update['weather_Rain'] = 2 #moderate rain
            if '+RA' in weather_list or '+DZ' in weather_list or '+SH' in weather_list or '+FZRA' in weather_list: 
                update['weather_Rain'] = 3 #heavy rain
            

            #Types of snow/hail/ice
            update['weather_SnowHailIce'] = 0 #none
            if '-SN' in weather_list or '+SG' in weather_list or '-GS' in weather_list or '-GR' in weather_list or '-PL' in weather_list or '-IC' in weather_list:
                update['weather_SnowHailIce'] = 1 #light
            if 'SN' in weather_list or '+SG' in weather_list or 'GS' in weather_list or 'GR' in weather_list or 'PL' in weather_list or 'IC' in weather_list:
                update['weather_SnowHailIce'] = 2 #moderate 
            if '+SN' in weather_list or '+SG' in weather_list or '+GS' in weather_list or '+GR' in weather_list or '+PL' in weather_list or '+IC' in weather_list:
                update['weather_SnowHailIce'] = 3 #heavy             
            

            #Types of fog/mist
            update['weather_Fog'] = 0 #none
            if '-FG' in weather_list or '-BR' in weather_list or '-HZ' in weather_list:
                update['weather_Fog'] = 1 #light
            if 'FG' in weather_list or 'BR' in weather_list or 'HZ' in weather_list:
                update['weather_Fog'] = 2 #moderate 
            if '+FG' in weather_list or '+BR' in weather_list or '+HZ' in weather_list or 'FG+' in weather_list:
                update['weather_Fog'] = 3 #heavy 
            
            
            
        #otherwise, no weather data, fill gues with avg values        
        else:
            #numerical fields, change 99999 to avg values
            update['weather_WetBulbFarenheit'] = 70
            update['weather_WindSpeed'] = 0
            update['weather_Visibility'] = 10
            update['weather_HourlyPrecip'] = 0            

            #string fields
            update['weather_SkyCondition'] = 'NA'   
            update['weather_WeatherType'] = 'NA'
            update['weather_Rain'] = 0
            update['weather_SnowHailIce'] = 0
            update['weather_Fog'] = 0
            
    return update


# ### PySpark Elasticsearch Create Grid
# 
# The following cells create the grid from scratch and upload to Elasticsearch.

# In[7]:

def create_full_feature_grid(index="dataframe",doc_type="rows"):
    #input: index name, document type
    #output: creates a new grid based on earliest and latest collision records
    start = dt.datetime.now()
    
    es_write_conf = {
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.resource" : "%s/%s" % (index,doc_type),
        "es.mapping.id" : "grid_id"
    } 
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    first_coll = upload_to_Elasticsearch.get_first_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
    first_coll_time = parse(first_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record
    rnd_start = dt.datetime(first_coll_time.year,first_coll_time.month,first_coll_time.day,first_coll_time.hour,0,0)

    last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
    last_coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record
    rnd_stop = dt.datetime(last_coll_time.year,last_coll_time.month,last_coll_time.day,last_coll_time.hour,0,0)
    
    t_delta = dt.timedelta(seconds=3600) #one hour
    next_time = rnd_start
    times = []
    while next_time <= rnd_stop:
        times.append(next_time)
        next_time+=t_delta
    #print len(times)
    #grid = sc.parallelize(combination_iter(grid_fullDate=times,grid_zipcode=zip_codes)).map(feature_grid)
    grid = sc.parallelize(times).flatMap(feature_grid)
    
    grid.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf)
    


# ### PySpark Elasticsearch Update Grid
# 
# The following cells read data from and write data to Elasticsearch.

# In[8]:

def add_fields_to_grid(grid_index,grid_doc,new_index=None,new_doc_type=None):
    #input: grid index and doc type, OPTIONAL new index name, document type
    #output: updates the existing grid with new fields
    
    if new_index and new_doc_type: 
        index,doc_type = new_index,new_doc_type
    else:
        index,doc_type=grid_index,grid_doc
        
    #Configuration for reading and writing to ES
    es_read_conf = { 
            "es.resource" : "%s/%s" % (grid_index,grid_doc), 
            "es.nodes" : ES_hosts, 
            "es.net.http.auth.user" : ES_username, 
            "es.net.http.auth.pass" : ES_password 
        }

    es_write_conf = {
            "es.nodes" : ES_hosts,
            "es.port" : "9200",
            "es.net.http.auth.user" : ES_username, 
            "es.net.http.auth.pass" : ES_password,
            "es.resource" : "%s/%s" % (index,doc_type),
            "es.mapping.id" : "grid_id"
        } 
    
    
    #Reads data from ES index into a Spark RDD (in the format (key, document))
    grid = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_read_conf)

    
    ## THIS IS WHERE YOU ADD NEW FIELDS ##
    #run updates via map functions to add new variables
    grid.map(update_functions)
 
    #write the new grid to Elasticsearch
    grid.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf)

    
def create_new_grid_timestamps(index="dataframe",doc_type="rows"):
    #input: index name, document type
    #output: creates a new grid based on latest collision records
    start = dt.datetime.now()
    
    es_write_conf = {
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.resource" : "%s/%s" % (index,doc_type),
        "es.mapping.id" : "grid_id"
    } 
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)

    #start at the latest grid time
    first_coll = upload_to_Elasticsearch.get_latest_record(index=index,doc_type=doc_type,datetime_field='grid_fullDate')
    if first_coll:
        #if there is a latest grid time
        first_coll_time = parse(first_coll['grid_fullDate']).replace(tzinfo=None) #get the date of the latest collision record
        rnd_start = dt.datetime(first_coll_time.year,first_coll_time.month,first_coll_time.day,first_coll_time.hour,0,0)
    else:
        #otherwise start from the earliest collision
        first_coll = upload_to_Elasticsearch.get_first_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
        first_coll_time = parse(first_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the first collision record
        rnd_start = dt.datetime(first_coll_time.year,first_coll_time.month,first_coll_time.day,first_coll_time.hour,0,0)

    #get the latest collision time
    last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
    last_coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record
    rnd_stop = dt.datetime(last_coll_time.year,last_coll_time.month,last_coll_time.day,last_coll_time.hour,0,0)
    
    t_delta = dt.timedelta(seconds=3600) #one hour
    next_time = rnd_start
    times = []
    while next_time <= rnd_stop:
        times.append(next_time)
        next_time+=t_delta
    #print len(times)
    #grid = sc.parallelize(combination_iter(grid_fullDate=times,grid_zipcode=zip_codes)).map(feature_grid)
    grid = sc.parallelize(times).flatMap(feature_grid)
    
        
    grid.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf)
    


# ### Run Grid creation and update code here

# In[9]:

create_new_grid_timestamps(index='nyc_dataframe',doc_type='rows')


# In[ ]:



