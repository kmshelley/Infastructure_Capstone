import requests
import urllib
import geojson
import csv
import ConfigParser


#read in the config file
config = ConfigParser.ConfigParser()
config.read('../config/capstone_config.ini')

api_key = config.get('GoogleAPI','api_key')


def get_geojson_position(address):
    #input: address as string
    #output: geojson point representing the location using Google GeoEncoding API
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s' % (urllib.quote_plus(address),api_key)
    r = requests.get(url)
    if r.status_code==200:
        results = r.json()['results']
        if len(results) > 0: return geojson.Point((results[0]['geometry']['location']['lng'],results[0]['geometry']['location']['lat']))
    return False
    
def geojson_from_address_file(filename,address_field):
    #input: csv filename of addresses, name of address field
    #output: Geojson feature Collection of points
    features=[]
    with open(filename,'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #create geojson point feature from row address, with properties from the row data
            point = get_geojson_position(row[address_field])
            if point: features.append(geojson.Feature(geometry=point,properties=row))
    return geojson.FeatureCollection(features)


    
