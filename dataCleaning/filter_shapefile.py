#lat/lon grid class
import os
import math
import datetime as dt
import LatLon as ll
import geojson
import shapefile
from shapely.geometry import LineString,Point,Polygon
from pyproj import Proj

def filter_shapefile(shp,poly,p=None,complete=False):
    #input: filename of shapefile, list of coordinates {(lng,lat) tuples} representing a polygon, a projection method
    #output: geojson of filtered shapefile elements
    if p:
        filter_poly = Polygon([p(lng,lat) for (lng,lat) in poly])
    else:
        filter_poly = Polygon([(x,y) for (x,y) in poly])
    features = []    
    sf = shapefile.Reader(shp)
    for shape in sf.iterShapes():
        rec = sf.iterRecords().next()
        _type = shape.shapeType

        if len(shape.points) > 0:
            if _type == 1:
                shape2 = Point(shape.points)
            if _type == 3:
                shape2 = LineString(shape.points)
            if _type == 5:
                shape2 = Polygon(shape.points)
                
            if filter_poly.intersects(shape2):
                props = {k:v for k,v in zip([f[0] for f in sf.fields],rec)}
                if p: 
                    coords = [p(x,y,inverse=True) for x,y in shape.points] #get the lng,lat coords
                else:
                    coords = shape.points

                if _type == 1:
                    #shape2 = Point(points)
                    try:
                        features.append(geojson.Feature(geometry=geojson.Point(coords[0]),properties=props))
                    except Exception as e:
                        print coords
                        print e
                if _type == 3:
                    try:
                        features.append(geojson.Feature(geometry=geojson.LineString(coords),properties=props))
                    except Exception as e:
                        print coords
                        print e
                if _type == 5:
                    try:
                        features.append(geojson.Feature(geometry=geojson.Polygon([coords]),properties=props))
                    except Exception as e:
                        print coords
                        print e

    return geojson.FeatureCollection(features)

def street_map_starts(geo):
    #input: geojson FeatureCollection of street map LineStrings
    #output: geojson FeatureCollection of single point representing the start point of the street
    points = []
    for feature in geo['features']:
        points.append(geojson.Feature(geometry=geojson.Point(list(geojson.utils.coords(feature))[0]),properties=feature['properties']))
    return geojson.FeatureCollection(points)
        
def street_map_ends(geo):
    #input: geojson FeatureCollection of street map LineStrings
    #output: geojson FeatureCollection of single point representing the end point of the street
    points = []
    for feature in geo['features']:
        points.append(geojson.Feature(geometry=geojson.Point(list(geojson.utils.coords(feature))[-1]),properties=feature['properties']))
    return geojson.FeatureCollection(points)

def street_map_centers(geo,p):
    #input: geojson FeatureCollection of street map LineStrings and a projection method to use prior to calculating the center
    #output: geojson FeatureCollection of single point representing the center point of the street
    points = []
    for feature in geo['features']:
        linestr = LineString([p(lng,lat) for lng,lat in list(geojson.utils.coords(feature))])
        c = linestr.centroid
        points.append(geojson.Feature(geometry=geojson.Point(p(c.x,c.y,inverse=True),properties=feature['properties'])))
    return geojson.FeatureCollection(points)

