#lat/lon grid class
import os
import math
import datetime as dt
import LatLon as ll
import geojson
import shapefile
from shapely.geometry import LineString,Point,Polygon
from pyproj import Proj


def shapefile_to_geojson(shp_file,p=None):
    #input: shapefile name, index name, document type, OPTIONAL pyproj projection
    #output: geojson feature collection

    ###ONLY WORKS FOR SINGLE TYPE SHAPEFILE####
    
    sf = shapefile.Reader(shp_file)
    features = []
    recs = sf.iterRecords()
    for shape in sf.iterShapes():
        rec = recs.next()
        props = {k:v for k,v in zip([f[0] for f in sf.fields[1:]],rec)}
        if p:
            coords = [p(x,y,inverse=True) for x,y in shape.points]
        else:
            coords = [(lng,lat) for lng,lat in shape.points]
        if shape.shapeType == 1:
            features.append(geojson.Feature(geometry=geojson.Point(coords[0]),properties=props))
            
        if shape.shapeType == 3:
            features.append(geojson.Feature(geometry=geojson.LineString(coords),properties=props))
            
        if shape.shapeType == 5:
            #if the type is a polygon, need to account for multi-polygons
            idx=0
            parts = []
            if len(shape.parts) == 1:
                #case where there is only one part
                features.append(geojson.Feature(geometry=geojson.MultiPolygon([[coords]]),properties=props))
            else:
                #multiple parts
                for i in range(1,len(shape.parts)):
                    part = coords[idx:shape.parts[i]]
                    part.append(coords[idx])
                    parts.append(part) #remember to close the polygon
                    idx=shape.parts[i]
                #add the last polygon part
                parts.append(coords[idx:])
                features.append(geojson.Feature(geometry=geojson.MultiPolygon([parts]),properties=props))

    return geojson.FeatureCollection(features)



def filter_shapefile(shp_file,poly,p=None,complete=False):
    #input: filename of shapefile, list of coordinates {(lng,lat) tuples} representing a polygon, a projection method
    #output: geojson of filtered shapefile elements
    if p:
        filter_poly = Polygon([p(lng,lat) for (lng,lat) in poly])
    else:
        filter_poly = Polygon([(x,y) for (x,y) in poly])
    features = []    
    sf = shapefile.Reader(shp_file)
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
                props = {k:v for k,v in zip([f[0] for f in sf.fields[1:]],rec)}
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

def filter_shapefile_return_points(shp_file,poly,p=None,complete=False):
    #input: filename of shapefile, list of coordinates {(lng,lat) tuples} representing a polygon, a projection method
    #output: geojson of filtered shapefile points
    if p:
        filter_poly = Polygon([p(lng,lat) for (lng,lat) in poly])
    else:
        filter_poly = Polygon([(x,y) for (x,y) in poly])
    features = []    
    sf = shapefile.Reader(shp_file)
    for shape in sf.iterShapes():
        rec = sf.iterRecords().next()
        _type = shape.shapeType

        for point in shape.points:
            if filter_poly.contains(Point(point)):
                props = {k:v for k,v in zip([f[0] for f in sf.fields[1:]],rec)}
                lng,lat = p(point[0],point[1],inverse=True)
                features.append(geojson.Feature(geometry=geojson.Point((lng,lat)),properties=props))
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


'''
shp_file = 'C:/Users/Katherine/Desktop/GIS/ArcMap_Data/street_segment_pub_shp/StreetSegment_Public_shp/StreetSegmentPublic.shp'

with open('C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/NYC_polygon.json','r') as geo_file:
    poly = list(geojson.utils.coords(geojson.load(geo_file)['features'][0]))
    
p = Proj(init='epsg:2263')
points = filter_shapefile_return_points(shp_file,poly,p)

with open('C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/NYC_street_points.json','w') as geo_file:
    geojson.dump(points,geo_file)
    
'''
shp_file = 'C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/shapefiles/nyc_zipcodes.shp'
geo = shapefile_to_geojson(shp_file)

with open('C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/nyc_zipcodes.json','w') as geo_file:
    geojson.dump(geo,geo_file)


