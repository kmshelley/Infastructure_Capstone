from classes.SearchGrid import SearchGrid
from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding
from dataAnalysis import driving_distance
from dataStorage import upload_to_Elasticsearch
from dataCleaning import filter_shapefile
import geojson,sys

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = './flatDataFiles/NY_Area_WBAN_Stations.json'
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(stations,index='weather_stations',doc_type='wban',delete_index=True,id_field='WBAN',geopoint='loc')

    #   Upload weather data to Elasticsearch   #
    ###CHANGE THESE FIELDS###
    #months = range(12,0,-1)
    #years = range(2016,2010,-1)
    #acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######


    #   Upload streetmap data to Elasticsearch   #
    with open('./flatDataFiles/NYC_streets_starts.json','r') as geo:
        streets = geojson.load(geo)['features']
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(streets,index='nyc_street_start',doc_type='start_points',geopoint='coords')

    #street segment centers    
    with open('./flatDataFiles/NYC_streets_centers.json','r') as geo:
        streets = geojson.load(geo)['features']
    upload_to_Elasticsearch.bulk_upload_docs_to_ES_cURL(streets,index='nyc_street_center',doc_type='center_points',geopoint='coords')


