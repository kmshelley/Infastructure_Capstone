#lat/lon grid class
import os
import math
import datetime as dt
import LatLon as ll
import geojson
import shapefile
from shapely.geometry import LineString,Point,Polygon,MultiPolygon
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
            if len(shape.parts) == 1:
                try:
                    features.append(geojson.Feature(geometry=geojson.Polygon([coords]),properties=props))
                except Exception as e:
                    #print coords
                    print e
            else:
                idx=0
                for i in range(1,len(shape.parts)):
                    if i < len(shape.parts):
                        part = coords[idx:shape.parts[i]]
                        props['ZCTA5CE10'] = props['ZCTA5CE10'] + '-' + str(i)
                        idx = shape.parts[i]
                        try:
                            features.append(geojson.Feature(geometry=geojson.Polygon([part]),properties=props))
                        except Exception as e:
                            print e
                    else:
                        part = coords[idx:]
                        props['ZCTA5CE10'] = props['ZCTA5CE10'] + '-' + str(len(shape.parts))
                        try:
                            features.append(geojson.Feature(geometry=geojson.Polygon([part]),properties=props))
                        except Exception as e:
                            print e

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
    records = sf.iterRecords()
    for shape in sf.iterShapes():
        rec = records.next()
        _type = shape.shapeType

        if p: 
            coords = [p(lng,lat) for lng,lat in shape.points] #get the lng,lat coords
        else:
            coords = shape.points
                    
        if len(shape.points) > 0:
            if _type == 1:
                parts = Point(coords)
            if _type == 3:
                parts = LineString(coords)
            if _type == 5:
                if len(shape.parts) == 1:
                    shape2 = Polygon(coords)
                else:
                    idx=0
                    parts = []
                    for i in range(1,len(shape.parts)):
                        part = coords[idx:shape.parts[i]]
                        parts.append(Polygon(part))
                        idx=shape.parts[i]
                    part = coords[idx:]
                    parts.append(Polygon(part))
                    
                    shape2 = MultiPolygon(parts)

                
            if filter_poly.contains(shape2.centroid):     
                fields = {k:v for k,v in zip([f[0] for f in sf.fields[1:]],rec)}
                zipcode = fields['ZCTA5CE10']
                points = shape.points
                if _type == 1:
                    #shape2 = Point(points)
                    try:
                        features.append(geojson.Feature(geometry=geojson.Point(tuple(points[0])),properties={'zipcode':zipcode}))
                    except Exception as e:
                        #print shape.points
                        print e
                if _type == 3:
                    try:
                        features.append(geojson.Feature(geometry=geojson.LineString([(pt1,pt2) for pt1,pt2 in points]),properties={'zipcode':zipcode}))
                    except Exception as e:
                        #print coords
                        print e
                if _type == 5:
                    if len(shape.parts) == 1:
                        geom = geojson.Polygon([[(pt1,pt2) for pt1,pt2 in points]])
                        features.append(geojson.Feature(geometry=geom,properties={'zipcode':zipcode}))
                    else:
                        for i in range(len(shape.parts)):                            
                            if i < len(shape.parts)-1:
                                part = points[shape.parts[i]:shape.parts[i+1]]  
                            else:
                                part = points[shape.parts[-1]:]
                                
                            geom = geojson.Polygon([[(pt1,pt2) for pt1,pt2 in part]])
                            feat = geojson.Feature(geometry=geom,properties={'zipcode':zipcode+ '-' + str(i+1)})
                            
                            features.append(feat)
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



shp_file = 'C:/Users/Katherine/Google Drive/DataSciW210/Final/datasets/NYC_polygon/NY_area_zips.shp'

##zips = shapefile_to_geojson(shp_file)
##with open('E:/GoogleDrive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/nyc_zip_codes.json','w') as geo_file:
##    geojson.dump(zips,geo_file)
    
with open('C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/NYC_polygon.json','r') as geo_file:
    poly = list(geojson.utils.coords(geojson.load(geo_file)['features'][0]))
    
p = Proj(init='epsg:2263')
zips = filter_shapefile(shp_file,poly,p)

with open('C:/Users/Katherine/Google Drive/DataSciW210/Final/Infrastructure_Capstone/flatDataFiles/nyc_zip_codes.json','w') as geo_file:
    geojson.dump(zips,geo_file)
    

