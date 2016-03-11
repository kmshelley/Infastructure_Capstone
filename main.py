__author__ = 'Katherine'

from dataAcquisition import open_data_api, acquire_QCLCD_data, acquire_NYC_Collisions
from dataStorage import upload_to_Elasticsearch
from dataCleaning import find_closest_geo_record
import ConfigParser
import time, datetime as dt
from dateutil.parser import parse
import geojson
import csv
from pyproj import Proj


def reload_zip_codes():
    ## RELOAD ZIP CODES AND COLLISIONS
    with open('./flatDataFiles/nyc_zip_codes.json', 'r') as geofile:
        zips = geojson.load(geofile)['features']
        
    #replace zip code definitions
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(zips,index='nyc_zip_codes',doc_type='zip_codes',id_field='zipcode',geoshape='coords',delete_index=True)

    #add closest WBAN to zip codes
    find_closest_geo_record.find_closest(index1='nyc_zip_codes',
                                         doc_type1='zip_codes',
                                         geo_field1='coords',
                                         id_field1='zipcode',
                                         index2='weather_stations',
                                         doc_type2='wban',
                                         geo_field2='loc',
                                         proj=Proj(init='epsg:2263'))
    #add closest EMS to zip codes
    find_closest_geo_record.find_closest(index1='nyc_zip_codes',
                                         doc_type1='zip_codes',
                                         geo_field1='coords',
                                         id_field1='zipcode',
                                         index2='emergency_stations',
                                         doc_type2='ems',
                                         geo_field2='loc',
                                         proj=Proj(init='epsg:2263'))
    #add closest Trauma Center to zip codes
    find_closest_geo_record.find_closest(index1='nyc_zip_codes',
                                         doc_type1='zip_codes',
                                         geo_field1='coords',
                                         id_field1='zipcode',
                                         index2='emergency_stations',
                                         doc_type2='trauma_centers',
                                         geo_field2='loc',
                                         proj=Proj(init='epsg:2263'))

def reload_all_collisions():
    #add all collisions
    with open('./flatDataFiles/collisions.csv','rb') as csvfile:
        docs = csv.DictReader(csvfile)
        acquire_NYC_Collisions.upload_collision_data_from_flatfile(docs,index='saferoad',doc_type='collisions',new_mapping=True)
    
    
def daily_update():
    #Updates weather and collisions data indefinitely
    while True:
        start = dt.datetime.now()
   
        #   Upload weather data to Elasticsearch   #
        acquire_QCLCD_data.collect_and_store_weather_data(months=[start.month],years=[start.year])
        

        #   Upload collisions data to Elasticsearch   #
        #   Upload latest collisions data to Elasticsearch   #
        last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
        coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

        #query all collisions from 7 days prior to the latest record, in case earlier records are late being uploaded or updated
        soql = "date > '%s'" % dt.datetime.strftime(coll_time - dt.timedelta(days=7),'%Y-%m-%dT%H:%M:%S')
        collisions = None
        while not collisions:
            try:
                collisions = open_data_api.get_open_data(nyc_url,vision_zero_endpoint,api_key,limit=5000,query=soql)
            except:
                #sometimes connection to Socrata doesn't work, wait 6 mintues and try again
                print "Failed to access Socrata. Sleeping for 1 minute."
                time.sleep(60) 
        acquire_NYC_Collisions.upload_collision_data_from_socrata(collisions,index='saferoad',doc_type='collisions')
        

        #   Update the grid    #
        

        
        end = dt.datetime.now()
        time.sleep(86400 - (end-start).total_seconds()) #run again in 24 hours
            
if __name__ == '__main__':
    '''Main Entry Point to the Program'''
    #read in the config file
    config = ConfigParser.ConfigParser()
    config.read('./config/capstone_config.ini')

    #NYC Open Data portal
    api_key = config.get('Socrata','api_key')
    nyc_url = config.get('NYC Portal','url')
    vision_zero_endpoint = config.get('NYC Portal','vision_zero')

    ny_state_url = config.get('NY State Portal','url')
    registration = config.get('NY State Portal','car_registration')

    ES_url = config.get('ElasticSearch','host')
    ES_password = config.get('ElasticSearch','password')
    ES_username= config.get('ElasticSearch','username')

    zip_codes = config.get('zip codes','zip_codes')

    #reload collisions
    reload_all_collisions()

    #run the daily update
    #daily_update()


