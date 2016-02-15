import requests
import urllib
import geojson
import csv
import ConfigParser
import datetime as dt


#read in the config file
config = ConfigParser.ConfigParser()
config.read('./config/capstone_config.ini')

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


def google_drive_distance(origin,destination):
    #input: a string representing an address, or lng-lat pair for two locations
    #output: Google Maps estimated drive distance (meters)
    if type(origin) is list:
        orig = '%s, %s' % (str(origin[1]),str(origin[0])) #convert lng-lat list to string
    else:
        origin = orig
    if type(destination) is list:
        dest = '%s, %s' % (str(destination[1]),str(destination[0])) #convert lng-lat list to string
    else:
        dest = destination

    url = 'https://maps.googleapis.com/maps/api/directions/json?origin=%s&destination=%s&key=%s' % (urllib.quote_plus(orig),urllib.quote_plus(dest),api_key)
    
    r = requests.get(url)
    if r.status_code==200:
        results = r.json()
        if len(results['routes']) > 0 and  results['status'] == 'OK':
            total_dist = 0
            for leg in results['routes'][0]['legs']:
                total_dist+=leg['distance']['value']
            return total_dist
    
    return False

