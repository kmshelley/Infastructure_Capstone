#lat/lon grid class
import os
import math
from shapely.geometry import Polygon
from pyproj import Proj
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import indices
from dataStorage import upload_to_Elasticsearch
import ConfigParser
from pprint import pprint
from copy import deepcopy
import datetime as dt
from dateutil.parser import parse

#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')


def test():
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    proj = Proj(init='epsg:2263')
    updates = []
    idx=0
    for res in helpers.scan(es,index='dataframe',doc_type='rows'):
        idx+=1
        if idx < 5:
            update = deepcopy(res['_source'])
            date_hour = parse(res['_source']['grid_fullDate'])
            query = '{ \
                      "query": { \
                        "bool": { \
                          "must": { \
                            "wildcard": { "ZCTA5CE10" : "%s*" } \
                          } \
                        } \
                      } \
                    }' % res['_source']['grid_zipcode']
            
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
                    wban = shape['_source']['closest_weather_stations']
            
            #find weather observations for the nearest weather station and time
            query2 = '''{
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
                    }''' % (wban,dt.datetime.strftime(date_hour,'%m/%d/%Y %H:%M'),dt.datetime.strftime(date_hour + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M'))


            observations = list(helpers.scan(es,query=query2,index='weather',doc_type='hourly_obs')) #get the first observation returned
            print "Zip: " + str(res['_source']['grid_zipcode'])
            print "Grid time: " + dt.datetime.strftime(date_hour,'%m/%d/%Y %H:%M')
            print "WBAN: " + wban
            print "%s weather observations found!" % str(len(observations))
            

            if len(observations) > 0:
            #numerical fields, change 99999 values to NA
                print observations[0]['_source']['weather_obs_id']
                print observations[0]['_source']['weather_WetBulbFarenheit']
                print observations[0]['_source']['weather_WindSpeed'] 
                print  observations[0]['_source']['weather_Visibility'] 
                print observations[0]['_source']['weather_HourlyPrecip']      


            
        else:
            break
    
def add_weather():
    es_url = 'http://%s:%s@%s:9200' % (ES_username,ES_password,ES_url)
    es = Elasticsearch(es_url)
    
    proj = Proj(init='epsg:2263')
    updates = []
    idx=0
    for res in helpers.scan(es,index='dataframe',doc_type='rows'):
        idx+=1
        update = deepcopy(res['_source'])
        date_hour = parse(res['_source']['grid_fullDate'])
        query = '{ \
                  "query": { \
                    "bool": { \
                      "must": { \
                        "wildcard": { "ZCTA5CE10" : "%s*" } \
                      } \
                    } \
                  } \
                }' % res['_source']['grid_zipcode']
        
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
                wban = shape['_source']['closest_weather_stations']
        
        #find weather observations for the nearest weather station and time
        query2 = '''{
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
                }''' % (wban,dt.datetime.strftime(date_hour,'%m/%d/%Y %H:%M'),dt.datetime.strftime(date_hour + dt.timedelta(seconds=3600),'%m/%d/%Y %H:%M'))


        observations = list(helpers.scan(es,query=query2,index='weather',doc_type='hourly_obs')) #get the first observation returned
        if len(observations) > 0:
            #numerical fields, change 99999 values to NA
            update['weather_WetBulbFarenheit'] = observations[0]['_source']['weather_WetBulbFarenheit'] if observations[0]['_source']['weather_WetBulbFarenheit']<> 99999 else 'NA'
            update['weather_WindSpeed'] = observations[0]['_source']['weather_WindSpeed'] if observations[0]['_source']['weather_WindSpeed']<> 99999 else 'NA'
            update['weather_Visibility'] = observations[0]['_source']['weather_Visibility'] if observations[0]['_source']['weather_Visibility']<> 99999 else 'NA'
            update['weather_HourlyPrecip'] = observations[0]['_source']['weather_HourlyPrecip'] if observations[0]['_source']['weather_HourlyPrecip']<> 99999 else 'NA'            

            #string fields
            
            update['weather_SkyCondition'] = observations[0]['_source']['weather_SkyCondition'] #Adjust this for just condition?
            update['weather_WeatherType'] = observations[0]['_source']['weather_WeatherType']

            weather_list = observations[0]['_source']['weather_WeatherType'].split() #space delimited list
            #Types of rain
            if '+RA' in weather_list or '+DZ' in weather_list or '+SH' in weather_list or '+FZRA' in weather_list: 
                update['weather_Rain'] = 3 #heavy rain
            if 'RA' in weather_list or 'DZ' in weather_list or 'SH' in weather_list or 'FZRA' in weather_list: 
                update['weather_Rain'] = 2 #moderate rain
            if '-RA' in weather_list or '-DZ' in weather_list or '-SH' in weather_list or '-FZRA' in weather_list: 
                update['weather_Rain'] = 1 #light rain
            else:
                update['weather_Rain'] = 0 #no rain

            #Types of snow/hail/ice
            if '+SN' in weather_list or '+SG' in weather_list or '+GS' in weather_list or '+GR' in weather_list or '+PL' in weather_list or '+IC' in weather_list:
                update['weather_SnowHailIce'] = 3 #heavy 
            if 'SN' in weather_list or '+SG' in weather_list or 'GS' in weather_list or 'GR' in weather_list or 'PL' in weather_list or 'IC' in weather_list:
                update['weather_SnowHailIce'] = 2 #moderate 
            if '-SN' in weather_list or '+SG' in weather_list or '-GS' in weather_list or '-GR' in weather_list or '-PL' in weather_list or '-IC' in weather_list:
                update['weather_SnowHailIce'] = 1 #light
            else:
                update['weather_SnowHailIce'] = 0 #none

            #Types of fog/mist
            if '+FG' in weather_list or '+BR' in weather_list or '+HZ' in weather_list or 'FG+' in weather_list:
                update['weather_Fog'] = 3 #heavy 
            if 'FG' in weather_list or 'BR' in weather_list or 'HZ' in weather_list:
                update['weather_Fog'] = 2 #moderate 
            if '-FG' in weather_list or '-BR' in weather_list or '-HZ' in weather_list:
                update['weather_Fog'] = 1 #light
            else:
                update['weather_Fog'] = 0 #none
                
        else:
            #numerical fields, change 99999 values to NA
            update['weather_WetBulbFarenheit'] = 'NA'
            update['weather_WindSpeed'] = 'NA'
            update['weather_Visibility'] = 'NA'
            update['weather_HourlyPrecip'] = 'NA'            

            #string fields
            update['weather_SkyCondition'] = 'NA'   
            update['weather_WeatherType'] = 'NA'
            update['weather_Rain'] = 'NA'
            update['weather_SnowHailIce'] = 'NA'
            update['weather_Fog'] = 'NA'

        updates.append(update)
        #update 10k records at a time
        if idx >= 10000:
            upload_to_Elasticsearch.update_ES_records_curl(updates,index='dataframe',doc_type='rows',id_field='grid_id')
            idx=0
            updates = []
    #upload remaining records
    upload_to_Elasticsearch.update_ES_records_curl(updates,index='dataframe',doc_type='rows',id_field='grid_id')        
