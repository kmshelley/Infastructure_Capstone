#from google.transit import gtfs_realtime_pb2
from classes import gtfs_realtime_pb2
from classes import nyct_subway_pb2
import urllib
import json
import ConfigParser

#read in the config file
config = ConfigParser.ConfigParser()
config.read('E:/GoogleDrive/DataSciW210/Final/Infrastructure_Capstone/config/capstone_config.ini')
mta_api = config.get('MTA API','api_key')
subway_url = 'http://datamine.mta.info/mta_esi.php?key=%s&feed_id=1' % mta_api


def get_mta_status_update():
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib.urlopen(subway_url)
    feed.ParseFromString(response.read())
    status = []
    
    for entity in feed.entity:
        trip = {}
        if entity.HasField('vehicle'):
            #trip information
            if entity.vehicle.HasField('trip'):
                trip['trip_id'] = entity.vehicle.trip.trip_id,
                trip['start_date'] = entity.vehicle.trip.start_date
                trip['route_id'] = entity.vehicle.trip.route_id
            #add the vehicle information
            vehicle = {}
            if entity.vehicle.HasField('current_stop_sequence'):vehicle['current_stop_sequence'] = entity.vehicle.current_stop_sequence
            if entity.vehicle.HasField('current_status'):vehicle['current_status'] = entity.vehicle.current_status
            if entity.vehicle.HasField('timestamp'):vehicle['timestamp'] = entity.vehicle.timestamp
            if entity.vehicle.HasField('stop_id'):vehicle['stop_id'] = entity.vehicle.stop_id
            
            trip['vehicle'] = vehicle
            
        if entity.HasField('stop_time_update'):
            #if entity.trip_update.HasField('stop_time_update'):
            updates = []
            #get all stop ETA/ETD updates
            for stop in entity.stop_time_update:#.stop_time_update:
                #trip update
                update = {}
                if stop.HasField('stop_id'):update['stop_id'] = stop.stop_id
                if stop.HasField('arrival'):update['arrival'] = stop.arrival.time
                if stop.HasField('departure'):update['departure'] = stop.departure.time
            updates.append(update)

            trip['trip_updates'] = updates

        status.append(trip)
            

    with open('status.json','w') as outfile: json.dump(status,outfile)

