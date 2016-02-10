from dataAcquisition import acquire_QCLCD_data

__author__ = 'Katherine'

if __name__ == '__main__':
    '''Main Entry Point to the Program'''

    
    # Weather data collection
    ###CHANGE THESE FIELDS###
    temp_data_dir = 'E:/GoogleDrive/DataSciW210/Final/datasets'
    months = range(2,0,-1)
    years = range(2016,2015,-1)
    ######

    acquire_QCLCD_data.collect_and_store_weather_data(months,years)

