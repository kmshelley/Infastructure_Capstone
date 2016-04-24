__author__ = 'Katherine'
from dataAcquisition import open_data_api, acquire_QCLCD_data, acquire_NYC_Collisions
from dataStorage import upload_to_Elasticsearch, download_from_Elasticsearch, elasticsearch_backup_restore
from dataCleaning import find_closest_geo_record, collision_geoshape_update, add_ZCTA, zip_code_features
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
memory = config.get('Spark','memory_small')
parallelism = config.get('Spark','parallelism_small')
cores = config.get('Spark','cores_small')

#NYC Open Data portal
api_key = config.get('Socrata','api_key')
nyc_url = config.get('NYC Portal','url')
vision_zero_endpoint = config.get('NYC Portal','vision_zero')
nyc_311_endpoint = config.get('NYC Portal','nyc_311')

#NYC Open Data portal
ny_state_url = config.get('NY State Portal','url')
registration_endpoint = config.get('NY State Portal','car_registration')
liquor_endpoint = config.get('NY State Portal','liquor')

ES_url = config.get('ElasticSearch','host')
ES_password = config.get('ElasticSearch','password')
ES_username= config.get('ElasticSearch','username')

data_grid = config.get('indexes','grid')
results = config.get('indexes','results')
update = config.get('indexes','pred_update')
features = config.get('indexes','features')
diag = config.get('indexes','diagnostics')

zip_codes = config.get('zip codes','zip_codes').split(',')

def reload_geojson(f,index,doc_type,**kwargs):
    ## RELOAD GEOJSON FILE TO ES
    with open(f, 'r') as geofile:
        geo = geojson.load(geofile)['features']
        
    #replace zip code definitions
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(geo,index='index',doc_type='doc_type',**kwargs)

    
def reload_geo_indices():
    ## RELOAD ZIP CODES, WEATHER STATIONS, TRAUMA CENTERS, AND EMS STATIONS
    reload_geojson('./flatDataFiles/nyc_zip_codes.json',index='nyc_zip_codes',doc_type='zip_codes',id_field='zipcode',geoshape='coords',delete_index=True)
    reload_geojson('./flatDataFiles/NY_Area_WBAN_Stations.json',index='weather_stations',doc_type='wban',id_field='WBAN',geopoint='loc')
    reload_geojson('./flatDataFiles/FDNY_FireStations.json',index='emergency_stations',doc_type='ems',geopoint='loc')
    reload_geojson('./flatDataFiles/NYC_Trauma_Centers.json',index='emergency_stations',doc_type='trauma_centers',geopoint='loc')

    
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

def run_predictions():
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
           './dataAnalysis/saferoad_model.py']
           

    index = update.split('/')[0]
    doc_type = update.split('/')[1]
    try:
        upload_to_Elasticsearch.create_es_index_and_mapping_cURL(index=index,doc_type=doc_type)
    except:
        pass
    
    try:
        subprocess.check_output(cmd)
        timestamp = dt.datetime.strftime(dt.datetime.now(),'%Y-%m-%dT%H:%M:%S')
        doc = { 'Model_Update_FullDate' : timestamp, 'Model_Errors': 'None', 'update_id' : '1' }
        upload_to_Elasticsearch.update_ES_records_curl([doc],index=index,doc_type=doc_type,id_field='update_id')
        
    except subprocess.CalledProcessError as e:
        timestamp = dt.datetime.strftime(dt.datetime.now(),'%Y-%m-%dT%H:%M:%S')
        doc = { 'Model_Update_FullDate' : timestamp, 'Model_Errors': e.output, 'update_id' : '1' }
        upload_to_Elasticsearch.update_ES_records_curl([doc],index=index,doc_type=doc_type,id_field='update_id')
        
    print "Finished prediction."

        
def daily_update():
    #Updates weather and collisions data indefinitely
    while True:
        start = dt.datetime.now()
   
        #   Upload latest weather data to Elasticsearch   #
        acquire_QCLCD_data.collect_and_store_weather_data(months=[start.month],years=[start.year])

        #   Upload latest collisions data to Elasticsearch   #
        last_coll = upload_to_Elasticsearch.get_latest_record(index='saferoad',doc_type='collisions',datetime_field='collision_DATETIME_C')
        coll_time = parse(last_coll['collision_DATETIME_C']).replace(tzinfo=None) #get the date of the latest collision record

        search_time = dt.datetime.strftime(coll_time - dt.timedelta(days=10),'%Y-%m-%dT%H:%M:%S')
        #query all collisions from 10 days prior to the latest record, in case earlier records are late being uploaded or updated
        soql = "date > '%s'" % search_time
        collisions = None
        for i in range(10):
            #try to update the collisions 10 times, then move on.
            try:
                collisions = open_data_api.get_open_data(nyc_url,vision_zero_endpoint,api_key,limit=5000,query=soql)
                acquire_NYC_Collisions.upload_collision_data_from_socrata(collisions,index='saferoad',doc_type='collisions')
                break
            except Exception as e:
                #sometimes connection to Socrata doesn't work, wait 1 mintue and try again
                print e
                print "Failed to access Socrata. Sleeping for 1 minute."
                time.sleep(60)

        #get historical 311 requests
        search_time = dt.datetime.strftime(coll_time - dt.timedelta(days=365),'%Y-%m-%dT%H:%M:%S')
        soql = "incident_zip in%s AND complaint_type='Street Condition' AND created_date > '%s'" % (str(tuple(zip_codes)),search_time)
        for i in range(10):
            #try to update the collisions 10 times, then move on.
            try:
                upload = {'index':'311_requests','doc_type':'requests','id_field':'unique_key'}
                open_data_api.upload_open_data_to_Elasticsearch(nyc_url,nyc_311_endpoint,api_key,soql,upload)
                break
            except Exception as e:
                #sometimes connection to Socrata doesn't work, wait 1 mintue and try again
                print e
                time.sleep(60)

##        #get historical ny state auto registration
##        soql = "record_type='VEH' AND zip in%s AND reg_valid_date > %s)" % (str(tuple(zip_codes)),search_time)
##        for i in range(10):
##            #try to update the collisions 10 times, then move on.
##            try:
##                upload = {'index':'vehicle_reg','doc_type':'reg','id_field'='unique_key'}
##                open_data_api.upload_open_data_to_Elasticsearch(ny_state_url,registration_endpoint,api_key,query=soql,upload)
##                break
##            except:
##                #sometimes connection to Socrata doesn't work, wait 1 mintue and try again
##                print "Failed to access Socrata. Sleeping for 1 minute."
##                time.sleep(60)

        #get historical liquor licenses
        search_time = dt.datetime.strftime(coll_time - dt.timedelta(days=10),'%Y-%m-%dT%H:%M:%S')
        soql = "zip in%s AND license_original_issue_date > '%s'" % (str(tuple(zip_codes)),search_time)
        for i in range(10):
            #try to update the collisions 10 times, then move on.
            try:
                upload = {'index':'liquor_licenses','doc_type':'lic','id_field':'license_serial_number'}
                open_data_api.upload_open_data_to_Elasticsearch(ny_state_url,liquor_endpoint,api_key,query=soql,kwargs=upload)
                break
            except Exception as e:
                #sometimes connection to Socrata doesn't work, wait 1 mintue and try again
                print e
                print "Failed to access Socrata. Sleeping for 1 minute."
                time.sleep(60)
                
                
        #   Update the grid  - Uses Spark  #
        update_grid()

        # Run the Model #
        run_predictions()

        
        end = dt.datetime.now()
        print "Finished update. Took %s. Sleeping for 24 hours." % str(end-start)
        time.sleep(86400 - (end-start).total_seconds()) #run again in 24 hours

    
if __name__ == '__main__':
    '''Main Entry Point to the Program'''
    #Reloads data from flat files, then runs daily update and prediction.
    #Will take a couple of hours to complete!
    elasticsearch_backup_restore.restore_index_from_json(index="nyc_grid",doc_type="rows",dump_loc="./flatDataFiles/backup/nyc_grid",id_field='grid_id')
    elasticsearch_backup_restore.restore_index_from_json(index="saferoad_results",doc_type="rows",dump_loc="./flatDataFiles/backup/saferoad_results",id_field='grid_id')
    reload_geo_indices()
    reload_all_collisions()
    daily_update()

    
