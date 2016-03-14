__author__ = 'Katherine'

from dataAcquisition import open_data_api, acquire_QCLCD_data, acquire_NYC_Collisions
from dataStorage import upload_to_Elasticsearch
from dataCleaning import find_closest_geo_record, collision_geoshape_update
import ConfigParser
import time, datetime as dt
from dateutil.parser import parse
import os
import geojson
import csv
from pyproj import Proj
import subprocess


#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

#spark conf
SPARK_HOME = config.get('Spark','home')
memory = config.get('Spark','memory')
parallelism = config.get('Spark','parallelism')
cores = config.get('Spark','cores')

#NYC Open Data portal
api_key = config.get('Socrata','api_key')
nyc_url = config.get('NYC Portal','url')
vision_zero_endpoint = config.get('NYC Portal','vision_zero')

ny_state_url = config.get('NY State Portal','url')
registration = config.get('NY State Portal','car_registration')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

zip_codes = config.get('zip codes','zip_codes').split(',')

    
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
    p = subprocess.Popen(['wget','https://nycopendata.socrata.com/api/views/h9gi-nx95/rows.csv'])
    #subprocess.call('curl -XDELETE %s/%s' % (es,index))
    out, err = p.communicate()
    if err: print '\n' + err + '\n\n'

    #wait for the file to download
    p.wait()
            
    with open('rows.csv','rb') as csvfile:
        docs = csv.DictReader(csvfile)
        acquire_NYC_Collisions.upload_collision_data_from_flatfile(docs,index='saferoad',doc_type='collisions',new_mapping=True)

    #remove the csv file
    os.remove('rows.csv')

def update_grid():
    cmd = ['%s/bin/spark-submit' % SPARK_HOME,
           '--master',
           'spark://spark1:7077',
           '--executor-memory',
           memory,
           '--conf',
           'spark.default.parallelism=%s' % str(parallelism),
           '--conf',
           'spark.cores.max=%s' % str(cores),
           '--jars',
           '/usr/local/spark/jars/elasticsearch-hadoop-2.2.0.jar',
           './gridCreation/pysparkGrid.py']
           
    
    subprocess.call(cmd)

    print "Done creating grid!"
    
def daily_update():
    #Updates weather and collisions data indefinitely
    while True:
        start = dt.datetime.now()
   
        #   Upload latest weather data to Elasticsearch   #
        acquire_QCLCD_data.collect_and_store_weather_data(months=[start.month],years=[start.year])

        #   Upload latest collisions data to Elasticsearch   #
        last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
        coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

        #query all collisions from 10 days prior to the latest record, in case earlier records are late being uploaded or updated
        soql = "date > '%s'" % dt.datetime.strftime(coll_time - dt.timedelta(days=10),'%Y-%m-%dT%H:%M:%S')
        collisions = None
        for i in range(10):
            #try to update the collisions 10 times, then move on.
            try:
                collisions = open_data_api.get_open_data(nyc_url,vision_zero_endpoint,api_key,limit=5000,query=soql)
                acquire_NYC_Collisions.upload_collision_data_from_socrata(collisions,index='saferoad',doc_type='collisions')
                break
            except:
                #sometimes connection to Socrata doesn't work, wait 6 mintues and try again
                print "Failed to access Socrata. Sleeping for 1 minute."
                time.sleep(60)
                
        #   Update the grid  - Uses Spark  #
        update_grid()

        
        end = dt.datetime.now()
        print "Finished update. Took %s. Sleeping for 24 hours." % str(end-start)
        time.sleep(86400 - (end-start).total_seconds()) #run again in 24 hours
            
if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    #reload collisions
    #reload_all_collisions()

    #set zcta zipcode
    #collision_geoshape_update.add_zcta_zip_to_collisions()

    #update_grid()
    
    #run the daily update
    daily_update()


