from dataAcquisition import acquire_QCLCD_data

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    # Upload Weather Stations to ElasticSearch #
    #geofile = 'E:/GoogleDrive/DataSciW210/Final/datasets/NY_Area_WBAN_Stations.geojson'
    #acquire_QCLCD_data.upload_geojson_to_ES(geofile,'weather_stations','WBAN')


    # Weather data collection
    ###CHANGE THESE FIELDS###
    months = range(2,1,-1)
    years = range(2016,2015,-1)
    #acquire_QCLCD_data.delete_ES_records('weather_observations','weather_observations')
    acquire_QCLCD_data.collect_and_store_weather_data(months,years)
    ######
