from dataAcquisition import acquire_QCLCD_data, acquire_NYC_Collisions, address_geocoding
from dataStorage import upload_to_Elasticsearch

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
##    geofile = 'E:/GoogleDrive/DataSciW210/Final/datasets/NY_Area_WBAN_Stations.geojson'
##    import geojson
##    with open(geofile,'r') as geo:
##        stations = geojson.load(geo)['features']
##    upload_to_Elasticsearch.upload_docs_to_ES(stations,'weather_stations','weather_stations','WBAN','loc')

    
    import geojson
    #stations = address_geocoding.geojson_from_address_file('E:/GoogleDrive/DataSciW210/Final/datasets/FDNY_Firehouse_Listing_cleaned.csv','geoAddress')
    with open('E:/GoogleDrive/DataSciW210/Final/datasets/FDNY_FireStations.json','r') as geo:
        stations = geojson.load(geo)['features']
        
    upload_to_Elasticsearch.upload_docs_to_ES(stations,'emergency_stations','fdny','FacilityName','loc')

    # Weather data collection
    ###CHANGE THESE FIELDS###
##    months = range(12,11,-1)
##    years = range(2011,2010,-1)
##    acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######

    #collision data processing and upload
##    collisions = 'E:/GoogleDrive/DataSciW210/Final/datasets/collisions.csv'
##    acquire_NYC_Collisions.upload_collision_data(collisions,index="saferoad",doc_type="collisions")
