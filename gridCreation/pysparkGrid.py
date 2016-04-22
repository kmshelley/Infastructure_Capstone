
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
import time
from dateutil.parser import parse
import json, csv
from itertools import izip, product
import requests

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

data_grid = config.get('indexes','grid')
results = config.get('indexes','results')

zip_codes = config.get('zip codes','zip_codes').split(',')
broadcast_zip = sc.broadcast(zip_codes)

wu_api = config.get('Wunderground','api_keys').split(',')


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
        
        
        #add collisions, weather, 311, and liquor license data
        g = add_collisions(g)
        g = add_weather(g)
        g = add_current_311(g)
        g = add_current_liquor(g)
        g = add_zipcode_data(g)
        ### ADD ADDITIONAL FEATURE ENGINEERING FUNCTIONS HERE ###
        output.append((g['grid_id'],g))
    
    return output
    

def result_grid(timestamp):
    #create grid record
    #datetime fields
    output = []
    for zipcode in broadcast_zip.value:
        g = {}
        g['grid_zipcode'] = zipcode
        g['grid_id'] = dt.datetime.strftime(timestamp,'%m%d%H%M') + '_' + zipcode

        g['grid_realDateTime'] = dt.datetime.strftime(timestamp,'%m/%d/%Y %H:%M')
        g['grid_dateHourStr'] = dt.datetime.strftime(timestamp,'%m%d%H')
        g['grid_jsDateTime'] = dt.datetime.strftime(timestamp,'%Y/%m/%d')

        g['all_probability'] = float(0)
        g['pedestrian_probability'] = float(0)
        g['cyclist_probability'] = float(0)
        g['motorist_probability'] = float(0)
        
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
        
        
        #add weather, 311, and liquor license data
        g = add_weather_prediction(g)
        g = add_current_311(g)
        g = add_current_liquor(g)
        g = add_zipcode_data(g)
        #g = predict_probabilities(g)
    
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
                        "must" : { "term": { "collision_ZCTA_ZIP" : "%s"} },
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
    

def add_zipcode_data(g):
    #adds zipcode level data to the grid
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    update = deepcopy(g)

    query = '''{
                "fields":[  "area",
                            "5mph",
                            "15mph",
                            "25mph",
                            "35mph",
                            "45mph",
                            "55mph",
                            "65mph",
                            "85mph",
                            "median_speed_limit",
                            "total_road_length",
                            "total_road_count",
                            "bridges",
                            "tunnels",
                            "Median AADT",
                            "Average AADT"
                        ],
                "query": {
                        "regexp":{
                            "zipcode": "%s*"
                        }
                    }
                }
            }''' % (g['grid_zipcode'])

    area = 0
    proj = Proj(init='epsg:2263') #NY/Long Island UTM projection
    try:
        docs = list(helpers.scan(es,query=query,index='nyc_zip_codes',doc_type='zip_codes'))
        max_area=0
        for doc in docs:
            for key in doc['fields']:
                if key != 'area':
                    update[key]=doc['fields'][key][0]
                else:
                    if key not in update: update['zip_area'] = 0.0
                    update['zip_area']+=doc['fields'][key][0] #add up area

                    #keep the AADT data for the largest zip area
                    if doc['fields'][key][0]> max_area and 'Median AADT' in doc['fields']:
                        max_area = doc['fields'][key][0]
                        update["Median AADT"] = doc['fields']["Median AADT"][0]
                        update["Average AADT"] = doc['fields']["Average AADT"][0]
    except:
        pass
    
    return update


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
    
    query = '{"query": { "bool": { "must": { "wildcard": { "zipcode" : "%s*" } } } } }' % g['grid_zipcode']

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
        update['weather_Rain_Dummy'] = 0
        if '-RA' in weather_list or '-DZ' in weather_list or '-SH' in weather_list or '-FZRA' in weather_list: 
            update['weather_Rain'] = 1 #light rain
            update['weather_Rain_Dummy'] = 1
        if 'RA' in weather_list or 'DZ' in weather_list or 'SH' in weather_list or 'FZRA' in weather_list: 
            update['weather_Rain'] = 2 #moderate rain
            update['weather_Rain_Dummy'] = 1
        if '+RA' in weather_list or '+DZ' in weather_list or '+SH' in weather_list or '+FZRA' in weather_list: 
            update['weather_Rain'] = 3 #heavy rain
            update['weather_Rain_Dummy'] = 1


        #Types of snow/hail/ice
        update['weather_SnowHailIce'] = 0 #none
        update['weather_SnowHailIce_Dummy'] = 0 #none
        if '-SN' in weather_list or '+SG' in weather_list or '-GS' in weather_list or '-GR' in weather_list or '-PL' in weather_list or '-IC' in weather_list:
            update['weather_SnowHailIce'] = 1 #light
            update['weather_SnowHailIce_Dummy'] = 1 #dummy
        if 'SN' in weather_list or '+SG' in weather_list or 'GS' in weather_list or 'GR' in weather_list or 'PL' in weather_list or 'IC' in weather_list:
            update['weather_SnowHailIce'] = 2 #moderate
            update['weather_SnowHailIce_Dummy'] = 1 #dummy
        if '+SN' in weather_list or '+SG' in weather_list or '+GS' in weather_list or '+GR' in weather_list or '+PL' in weather_list or '+IC' in weather_list:
            update['weather_SnowHailIce'] = 3 #heavy
            update['weather_SnowHailIce_Dummy'] = 1 #dummy          


        #Types of fog/mist
        update['weather_Fog'] = 0 #none
        update['weather_Fog_Dummy'] = 0 #none
        if '-FG' in weather_list or '-BR' in weather_list or '-HZ' in weather_list:
            update['weather_Fog'] = 1 #light
            update['weather_Fog_Dummy'] = 1 #dummy
        if 'FG' in weather_list or 'BR' in weather_list or 'HZ' in weather_list:
            update['weather_Fog'] = 2 #moderate
            update['weather_Fog_Dummy'] = 1 #dummy
        if '+FG' in weather_list or '+BR' in weather_list or '+HZ' in weather_list or 'FG+' in weather_list:
            update['weather_Fog'] = 3 #heavy
            update['weather_Fog_Dummy'] = 1 #dummy

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
            update['weather_Rain_Dummy'] = 0
            if '-RA' in weather_list or '-DZ' in weather_list or '-SH' in weather_list or '-FZRA' in weather_list: 
                update['weather_Rain'] = 1 #light rain
                update['weather_Rain_Dummy'] = 1
            if 'RA' in weather_list or 'DZ' in weather_list or 'SH' in weather_list or 'FZRA' in weather_list: 
                update['weather_Rain'] = 2 #moderate rain
                update['weather_Rain_Dummy'] = 1
            if '+RA' in weather_list or '+DZ' in weather_list or '+SH' in weather_list or '+FZRA' in weather_list: 
                update['weather_Rain'] = 3 #heavy rain
                update['weather_Rain_Dummy'] = 1
            

            #Types of snow/hail/ice
            update['weather_SnowHailIce'] = 0 #none
            update['weather_SnowHailIce_Dummy'] = 0 #none
            if '-SN' in weather_list or '+SG' in weather_list or '-GS' in weather_list or '-GR' in weather_list or '-PL' in weather_list or '-IC' in weather_list:
                update['weather_SnowHailIce'] = 1 #light
                update['weather_SnowHailIce_Dummy'] = 1 #dummy
            if 'SN' in weather_list or '+SG' in weather_list or 'GS' in weather_list or 'GR' in weather_list or 'PL' in weather_list or 'IC' in weather_list:
                update['weather_SnowHailIce'] = 2 #moderate
                update['weather_SnowHailIce_Dummy'] = 1 #dummy
            if '+SN' in weather_list or '+SG' in weather_list or '+GS' in weather_list or '+GR' in weather_list or '+PL' in weather_list or '+IC' in weather_list:
                update['weather_SnowHailIce'] = 3 #heavy
                update['weather_SnowHailIce_Dummy'] = 1 #dummy
            

            #Types of fog/mist
            update['weather_Fog'] = 0 #none
            update['weather_Fog_Dummy'] = 0 #none
            if '-FG' in weather_list or '-BR' in weather_list or '-HZ' in weather_list:
                update['weather_Fog'] = 1 #light
                update['weather_Fog_Dummy'] = 1 #dummy
            if 'FG' in weather_list or 'BR' in weather_list or 'HZ' in weather_list:
                update['weather_Fog'] = 2 #moderate
                update['weather_Fog_Dummy'] = 1 #dummy
            if '+FG' in weather_list or '+BR' in weather_list or '+HZ' in weather_list or 'FG+' in weather_list:
                update['weather_Fog'] = 3 #heavy
                update['weather_Fog_Dummy'] = 1 #dummy
            
            
            
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

            update['weather_Rain_Dummy'] = 0
            update['weather_SnowHailIce_Dummy'] = 0
            update['weather_Fog_Dummy'] = 0
            
    return update

def dummy_weather_fields(g):
    #sets dummy weather fields
    #weather fields
    if g['weather_Fog']>0:
        g['weather_Fog_Dummy'] = 1
    else:
        g['weather_Fog_Dummy'] = 0
        
    if g['weather_Rain'] > 0:
        g['weather_Rain'] = 1
    else:
        g['weather_Rain'] = 0
        
    if g['weather_SnowHailIce']>0:
        g['weather_SnowHailIce'] = 1
    else:
        g['weather_SnowHailIce'] = 0

    return g
    
    
def wunderground_predictions():
    #get WeatherUnderground 10 day hourly forecast
    output=[]
    idx=0
    switch = len(zip_codes)/len(wu_api) + 1 #simple ratio of zip codes to api keys
    to_search = deepcopy(zip_codes)
    keys = deepcopy(wu_api)
    key = keys.pop()
    #search by zip code
    while len(to_search) > 0:
        idx+=1
        zipcode = to_search.pop()
        
        if idx%10 == 0:
            #if the current key has been used 10 times, switch to a new key
            key = keys.pop()
            
        wu_url='http://api.wunderground.com/api/%s/hourly10day/q/%s.json' % (key,zipcode)
        pred = requests.get(wu_url)
        if pred.status_code==200:
            for hour in pred.json()['hourly_forecast']:
                new_hour = {}
                #define a unique id for the forecast
                new_hour['pred_id'] = '%s%s%s%s_%s' % (hour['FCTTIME']['year'],
                                                       hour['FCTTIME']['mon_padded'],
                                                       hour['FCTTIME']['mday_padded'],
                                                       hour['FCTTIME']['hour_padded'],
                                                       zipcode)

                #add zip code for querying
                new_hour['pred_zipcode'] = zipcode
                
                #format datetime fields
                new_hour.update(hour['FCTTIME'])
                new_hour['hour'] = int(new_hour['hour_padded'])
                new_hour['day'] = int(new_hour['mday_padded'])
                new_hour['month'] = int(new_hour['mon_padded'])
                new_hour['year'] = int(new_hour['year'])
                new_hour['epoch'] = float(new_hour['epoch'])
                #define an ES formatted datetime field
                timestamp=dt.datetime(new_hour['year'],new_hour['month'],new_hour['day'],new_hour['hour'],0)
                new_hour['pred_fullDate'] = dt.datetime.strftime(timestamp,'%Y-%m-%dT%H:%M:%S')

                #format weather fields
                new_hour['temp'] = hour['temp']['english']
                new_hour['condition'] = hour['condition'].lower()
                new_hour['icon'] = hour['icon']
                new_hour['icon_url'] = hour['icon_url']
                new_hour['forecastStr'] = hour['icon'].lower()
                new_hour['windspeed'] = hour['wspd']['english']
                new_hour['precipitation'] = hour['qpf']['english']
                new_hour['prob_of_precip'] = hour['pop']
                new_hour['snowfall'] = hour['snow']['english']
                new_hour['pressure'] = hour['mslp']['english']

                #condition fields
                new_hour['pred_rain_dummy'] = 0
                if new_hour['condition'].find('rain') > -1 or new_hour['condition'].find('tstorms') > -1:
                    new_hour['pred_rain_dummy'] = 1
                    
                new_hour['pred_fog_dummy'] = 0
                if new_hour['condition'].find('fog') > -1 or new_hour['condition'].find('hazy') > -1:
                    new_hour['pred_fog_dummy'] = 1
                    
                new_hour['pred_snow_dummy'] = 0
                if new_hour['condition'].find('flurries') > -1 or new_hour['condition'].find('sleet') > -1 or new_hour['condition'].find('snow') > -1:
                    new_hour['pred_snow_dummy'] = 1
                    
                output.append((new_hour['pred_id'],new_hour))

        if len(keys) == 0:
            #if all the api keys have been exhausted, sleep for a minute, then start at the beginning of the api key list
            time.sleep(60) 
            keys = deepcopy(wu_api)
            key = keys.pop()
        
            
    return output    

def add_weather_prediction(g):
    #ADD CODE FOR WEATHER PREDICTIONS
    update = deepcopy(g)
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    time = parse(g['grid_fullDate']).replace(tzinfo=None)
    
    query = '''{
                "query" : {
                    "bool": {
                        "must": { "term" : { "pred_zipcode" : "%s" } },
                        "must" : {
                            "range" : {
                                "pred_fullDate" : {
                                    "gte": "%s",
                                    "lt": "%s",
                                    "format": "MM/dd/yyyy HH:mm"
                                }
                            }	
                        }	
                    }	
                }
            }''' % (g['grid_zipcode'],dt.datetime.strftime(time,'%m/%d/%Y %H:%M'),dt.datetime.strftime(time + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M'))

    
    observations = list(helpers.scan(es,query=query,index='wunderground',doc_type='hourly')) #get the first observation returned
    obs = None
    if len(observations) > 0:
        min_diff = float('inf')
        best_obs = None
        #iterate through the returned observations, add the closest observation in time
        for obs in observations:
            obs_time = parse(obs['_source']['pred_fullDate']).replace(tzinfo=None)
            if abs((obs_time - time).total_seconds()) < min_diff:
                best_obs = obs['_source']
                min_diff = abs((obs_time - time).total_seconds())
                                  
        obs = best_obs
                                
    if obs:
        #numerical fields
        try:
            update['weather_Temp'] = float(obs['temp'])
        except:
            update['weather_Temp'] = 70.0

        try:
            update['weather_WindSpeed'] = float(obs['windspeed'])
        except:
            update['weather_WindSpeed'] = 0.0

        try:
            update['weather_Precip'] = float(obs['precipitation'])
        except:
            update['weather_Precip'] = 0.0
            
        #bindary weather fields        
        update['weather_Rain_Dummy'] = obs['pred_rain_dummy']
        update['weather_SnowHailIce_Dummy'] = obs['pred_snow_dummy']
        update['weather_Fog_Dummy'] = obs['pred_fog_dummy']
        
    else:
        #numerical fields
        update['weather_Temp'] = 70.0
        update['weather_WindSpeed'] = 0.0
        update['weather_Precip'] = 0.0      

        #bindary weather fields  
        update['weather_Rain_Dummy'] = 0
        update['weather_SnowHailIce_Dummy'] = 0
        update['weather_Fog_Dummy'] = 0
    return update

def add_current_311(g):
    #CURRENT OPEN ROAD CONDITION COMPLAINTS
    update = deepcopy(g)

    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    time = parse(g['grid_fullDate']).replace(tzinfo=None)
    #find all 311 requests that were opened before the grid time, and closed after or are not yet closed
    hour_start =dt.datetime.strftime(time,'%m/%d/%Y %H:%M')
    hour_end = dt.datetime.strftime(time + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M')
    query = '''{
	"fields": ["created_date","status","closed_date","resolution_action_updated_date","due_date","ZCTA_ZIP_NoSuffix"],
	"query" : {
            "bool": {
                "must": { "term" : { "ZCTA_ZIP_NoSuffix" : "%s" } },
                "must" : {
                    "range" : {
                        "created_date" : {
                            "lte": "%s",
                            "format": "MM/dd/yyyy HH:mm"
                        }
                    }
                },
                "should":[
                    { "term" : { "status" : "open" } },
                    { "term" : { "status" : "pending" } },
                    { "bool": { 
                            "must": { "term" : { "status" : "closed" } } ,
                            "should":[
                                    { 
                                        "range" : {
                                            "closed_date" : {
                                                "gt": "%s",
                                                "format": "MM/dd/yyyy HH:mm"
                                            }
                                        } 
                                    },
                                    { 
                                        "range" : {
                                            "resolution_action_updated_date" : {
                                                "gt": "%s",
                                                "format": "MM/dd/yyyy HH:mm"
                                            }
                                        } 
                                    },
                                    { 
                                        "range" : {
                                            "due_date" : {
                                                "gt": "%s",
                                                "format": "MM/dd/yyyy HH:mm"
                                        }
                                    } 
                                }
                            ],
                            "minimum_should_match" : 1
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }''' % (g['grid_zipcode'],hour_start,hour_end,hour_end,hour_end)


    update['road_cond_requests']=0
    try:
        complaints = list(helpers.scan(es,query=query,index='311_requests',doc_type='requests')) #get the 311 road condition requests that were still open hour
        update['road_cond_requests']=len(complaints)
    except:
        pass
        
    return update

def add_current_liquor(g):
    #ADD CODE FOR CURRENT ACTIVE LIQUOR LICENSES
    update = deepcopy(g)
    
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    time = parse(g['grid_fullDate']).replace(tzinfo=None)
    #find all 311 requests that were opened before the grid time, and closed after or are not yet closed
    hour_start =dt.datetime.strftime(time,'%m/%d/%Y %H:%M')
    hour_end = dt.datetime.strftime(time + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M')
    query = '''{
                "fields": ["license_original_issue_date","license_expiration_date","ZCTA_ZIP_NoSuffix"],
                "query" : {
                    "bool": {
                        "must": { "term" : { "ZCTA_ZIP_NoSuffix" : "%s" } },
                        "must" : {
                            "range" : {
                                "license_original_issue_date" : {
                                    "lte": "%s",
                                    "format": "MM/dd/yyyy HH:mm"
                                }
                            }
                        },
                        "must": { 
                            "range" : {
                                "license_expiration_date" : {
                                    "gt": "%s",
                                    "format": "MM/dd/yyyy HH:mm"
                                }
                            }
                        }
                    }	
                }
            }''' % (g['grid_zipcode'],hour_start,hour_end)

    update['liquor_licenses']=0
    try:
        licenses = list(helpers.scan(es,query=query,index='liquor_licenses',doc_type='lic')) #get the active liquor licenses by zip
        update['liquor_licenses']=len(licenses)
    except:
        pass
    
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

    #create the index, set the replicas so uploads will not err out
    settings = {"settings": {"number_of_replicas" : 1} }
    p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es_url,index),'-d',json.dumps(settings)])
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'
    
    #make sure the mapping of the probability field is double
    mapping = {'properties':
               {
                    "15mph": { "type": "double" },
                    "25mph": { "type": "double" },
                    "35mph": { "type": "double" },
                    "45mph": { "type": "double" },
                    "55mph": { "type": "double" },
                    "5mph": { "type": "double" },
                    "65mph": { "type": "double" },
                    "85mph": { "type": "double" },
                    "bridges": { "type": "double" },
                    "grid_motoristFatalities": { "type": "double" },
                    "grid_motoristInjuries": { "type": "double" },
                    "grid_pedestrianFatalities": { "type": "double" },
                    "grid_pedestrianInjuries": { "type": "double" },
                    "grid_totalFatalities": { "type": "double" },
                    "grid_totalInjuries": { "type": "double" },
                    "liquor_licenses": { "type": "double" },
                    "median_speed_limit": { "type": "double" },
                    "road_cond_requests": { "type": "double" },
                    "total_road_length": { "type": "double" },
                    "tunnels": { "type": "double" },
                    "zip_area": { "type": "double" },
                    "Median AADT": { "type": "double" },
                    "Average AADT": { "type": "double" }
                 }
              }
    #use cURL to put the mapping
    p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es_url,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'

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
    

def create_results_grid(index="saferoad_results",doc_type="rows",offset=10):
    #input: index name, document type, day offset (default to 10 days)
    #output: creates a new grid based on earliest and latest collision records
    start = dt.datetime.now()

    es = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    #create the index, set the replicas so uploads will not err out
    settings = {"settings": {"number_of_replicas" : 1} }
    p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es,index),'-d',json.dumps(settings)])
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'
        
    #make sure the mapping of the probability field is double
    mapping = {'properties':
               {
                    "15mph": { "type": "double" },
                    "25mph": { "type": "double" },
                    "35mph": { "type": "double" },
                    "45mph": { "type": "double" },
                    "55mph": { "type": "double" },
                    "5mph": { "type": "double" },
                    "65mph": { "type": "double" },
                    "85mph": { "type": "double" },
                    "bridges": { "type": "double" },
                    "grid_motoristFatalities": { "type": "double" },
                    "grid_motoristInjuries": { "type": "double" },
                    "grid_pedestrianFatalities": { "type": "double" },
                    "grid_pedestrianInjuries": { "type": "double" },
                    "grid_totalFatalities": { "type": "double" },
                    "grid_totalInjuries": { "type": "double" },
                    "liquor_licenses": { "type": "double" },
                    "median_speed_limit": { "type": "double" },
                    "road_cond_requests": { "type": "double" },
                    "total_road_length": { "type": "double" },
                    "tunnels": { "type": "double" },
                    "zip_area": { "type": "double" },
                    "Median AADT": { "type": "double" },
                    "Average AADT": { "type": "double" },
                    "all_probability": { "type": "double" },
                    "pedestrian_probability": { "type": "double" },
                    "cyclist_probability": { "type": "double" },
                    "motorist_probability": { "type": "double" }
                 }
              }
    #use cURL to put the mapping
    p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'

    #updates an ES index with WeatherUnderground 10 day hourly forecasts for NYC zip codes
    wu_write_conf = {
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.resource" : "wunderground/hourly",
        "es.mapping.id" : "pred_id"
    }  
    predictions = sc.parallelize(wunderground_predictions())
    predictions.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=wu_write_conf)

    #Define prediction results grid (looks very similar to dataframe grid
    es_write_conf = {
        "es.nodes" : ES_hosts,
        "es.port" : "9200",
        "es.net.http.auth.user" : ES_username, 
        "es.net.http.auth.pass" : ES_password,
        "es.resource" : "%s/%s" % (index,doc_type),
        "es.mapping.id" : "grid_id"
    } 
    
    #define the start and end of a leap year for collecting all month-day combos
    startpoint = dt.datetime.now() #start of results grid
    endpoint = dt.datetime.now() + dt.timedelta(days=offset) #end point of grid
    
    start_date= dt.datetime(startpoint.year,startpoint.month,startpoint.day,0,0)
    end_date = dt.datetime(endpoint.year,endpoint.month,endpoint.day,0,0)

    t_delta = dt.timedelta(seconds=3600) #one hour
    next_date = start_date
    times = []
    while next_date < end_date:
        #dates.append(dt.datetime.strftime(next_date,'%m/%d %H:%S'))
        times.append(next_date)
        next_date+=t_delta
        
    grid = sc.parallelize(times).flatMap(result_grid)
    
    grid.saveAsNewAPIHadoopFile(
            path='-', 
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable", 
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
            conf=es_write_conf)

    
def reset_grid_collisions(rdd):
    #input: Spark Elasticsearch RDD
    #output: mapped RDD with new collision information
    return rdd.map(lambda (key,row): (key,add_collisions(row)))

def reset_grid_weather(rdd):
    #Input: Spark Elasticsearch RDD
    #output: mapped RDD with new weather information
    return rdd.map(lambda (key,row): (key,add_weather(row)))

def reset_grid_weather_Dummy(rdd):
    #Input: Spark Elasticsearch RDD
    #output: mapped RDD with new dummy weather information
    return rdd.map(lambda (key,row): (key,dummy_weather_fields(row)))

def reset_grid_311(rdd):
    #input: Spark Elasticsearch RDD
    #output: mapped RDD with new 311 information
    return rdd.map(lambda (key,row): (key,add_current_311(row)))

def reset_grid_liquor(rdd):
    #input: Spark Elasticsearch RDD
    #output: mapped RDD with new liquor license information
    return rdd.map(lambda (key,row): (key,add_current_liquor(row)))

def reset_zip_data(rdd):
    #input: Spark Elasticsearch RDD
    #output: mapped RDD with new liquor license information
    return rdd.map(lambda (key,row): (key,add_zipcode_data(row)))

# ### PySpark Elasticsearch Update Grid
# 
# The following cells read data from and write data to Elasticsearch.

# In[8]:

def add_fields_to_grid(grid_index,grid_doc,new_index=False,new_doc_type=False,query=None,functions=[]):
    #input: grid index and doc type, OPTIONAL new index name, document type, a list of functions to perform on the grid RDD
    #output: updates the existing grid with new fields based on functions provided
    
    if new_index and new_doc_type: 
        index,doc_type = new_index,new_doc_type
    else:
        index,doc_type=grid_index,grid_doc

    print "Writing data to %s/%s" % (index,doc_type)
        
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
    
    if query: es_read_conf['es.query'] = query
    
    #Reads data from ES index into a Spark RDD (in the format (key, document))
    grid = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_read_conf)

    
    ## THIS IS WHERE YOU ADD NEW FIELDS ##
    #run updates via provided functions to add new variables
    for f in functions:
        eval_str = '%s(grid)' % f #string defines function to run on the grid RDD
        grid = eval(eval_str) #evaluate function
    
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
        first_coll_time = parse(first_coll['grid_fullDate']).replace(tzinfo=None) - dt.timedelta(days=3) #get the date of the latest collision record
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
    
def create_grid_timestamps_from_start_end(start,end,index="dataframe",doc_type="rows"):
    #input: start time, end time, index name, document type
    #output: creates a new grid based on start and end timestamps
    
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

    #create the index, set the replicas so uploads will not err out
    settings = {"settings": {"number_of_replicas" : 1} }
    p = subprocess.Popen(['curl','-XPUT','%s/%s' % (es_url,index),'-d',json.dumps(settings)])
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'
    
    #make sure the mapping of the probability field is double
    mapping = {'properties':
               {
                    "15mph": { "type": "double" },
                    "25mph": { "type": "double" },
                    "35mph": { "type": "double" },
                    "45mph": { "type": "double" },
                    "55mph": { "type": "double" },
                    "5mph": { "type": "double" },
                    "65mph": { "type": "double" },
                    "85mph": { "type": "double" },
                    "bridges": { "type": "double" },
                    "grid_motoristFatalities": { "type": "double" },
                    "grid_motoristInjuries": { "type": "double" },
                    "grid_pedestrianFatalities": { "type": "double" },
                    "grid_pedestrianInjuries": { "type": "double" },
                    "grid_totalFatalities": { "type": "double" },
                    "grid_totalInjuries": { "type": "double" },
                    "liquor_licenses": { "type": "double" },
                    "median_speed_limit": { "type": "double" },
                    "road_cond_requests": { "type": "double" },
                    "total_road_length": { "type": "double" },
                    "tunnels": { "type": "double" },
                    "zip_area": { "type": "double" },
                    "Median AADT": { "type": "double" },
                    "Average AADT": { "type": "double" }
                 }
              }
    #use cURL to put the mapping
    p = subprocess.Popen(['curl','%s/%s/_mapping/%s' % (es_url,index,doc_type),'-d','%s' % json.dumps(mapping)],stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'

    t_delta = dt.timedelta(seconds=3600) #one hour
    next_time = start
    times = []
    while next_time <= end:
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
##add_fields_to_grid(grid_index="saferoad_results",grid_doc="rows",functions=["reset_zip_data","reset_grid_311"])
##
##for i in range(2016,2011,-1):
##    q = '''{
##            "query" : {
##                "bool": {			
##                    "must" : {
##                        "range" : {
##                            "grid_fullDate" : {
##                                "gte": "01/01/%s 00:00",
##                                "lt": "01/01/%s 00:00",
##                                "format": "MM/dd/yyyy HH:mm"
##                            }
##                        }	
##                    }	
##                }	
##            }
##        }''' % (str(i),str(i+1))
##
##    add_fields_to_grid(grid_index="nyc_grid",grid_doc="rows",query=q,functions=["reset_zip_data","reset_grid_311"])

create_new_grid_timestamps(index=data_grid.split('/')[0],doc_type=data_grid.split('/')[1])
create_results_grid(index=results.split('/')[0],doc_type=results.split('/')[1],offset=10)
