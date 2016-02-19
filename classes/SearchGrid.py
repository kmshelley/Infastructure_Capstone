#lat/lon grid class
import os
import math
import datetime as dt
import LatLon as ll
import simplekml

class SearchGrid():
    #defines the address search grid
    def __init__(self,bbox=[0,0,0,0],width=1):
        #input: bounding-box [sw-lng,sw-lat,ne-lng,ne-lat], N = # of grid squares
        #output: initiates the SearchGrid generator
        self.width = width #number of grid squares per row (grid is width X width)
        self.bbox = {'sw': ll.LatLon(bbox[1],bbox[0]),'ne':ll.LatLon(bbox[3],bbox[2])}#bounding box

        #characteristics of the grid
        self.diagonal_dist = self.bbox['sw'].distance(self.bbox['ne']) #distance between bounding coordinates
        self.diagonal_bearing = self.bbox['sw'].heading_initial(self.bbox['ne']) #bearing between bounding coordinates
        self.side_length = math.sqrt((self.diagonal_dist**2)/2) #width of bounding box (KM)
        #other bounding points of the box
        nw_ll = self.bbox['sw'].offset(self.diagonal_bearing - 45, self.side_length)
        se_ll = self.bbox['sw'].offset(self.diagonal_bearing + 45, self.side_length)
        
        #dictionary defining the points of the entire bounding box
        self.bounding_box = {
            'sw.lat': self.bbox['sw'].lat,
            'sw.lng': self.bbox['sw'].lon,
            'se.lat': se_ll.lat,
            'se.lng': se_ll.lon,
            'nw.lat': nw_ll.lat,
            'nw.lng': nw_ll.lon,
            'ne.lat': self.bbox['ne'].lat,
            'ne.lng': self.bbox['ne'].lon
            }

    def bounding_box_kml(self):
        #Creates a Google Earth KML of the address bounding-box
        kml = simplekml.Kml()
        coords = [
                (self.bounding_box['se.lng'],self.bounding_box['se.lat']),
                (self.bounding_box['sw.lng'],self.bounding_box['sw.lat']),
                (self.bounding_box['nw.lng'],self.bounding_box['nw.lat']),
                (self.bounding_box['ne.lng'],self.bounding_box['ne.lat']),
                (self.bounding_box['se.lng'],self.bounding_box['se.lat'])
                ]
        kml.newlinestring(name='bouding_box', description='bounding box',
                                coords=coords)
        kml.save(os.path.join(os.getcwd(),'bb.kml'))

    def grid_walk(self):
        #generator function for grid points
        #need to make more robust for crossing hemispheres
        grid_length = self.side_length/self.width #width of grid square

        ll_orig = ll.LatLon(self.bounding_box['sw.lat'],self.bounding_box['sw.lng'])
        ll1 = ll_orig#sw point of grid square
        for i in range(self.width):
            for j in range(self.width):
                ll2 = ll1.offset(self.diagonal_bearing, math.sqrt(2 * grid_length**2))#ne point of grid square
                ll_center = ll1.offset(self.diagonal_bearing, math.sqrt(2 * grid_length**2)/2) #center point of grid square

                nw_ll = ll1.offset(self.diagonal_bearing - 45, grid_length)
                se_ll = ll1.offset(self.diagonal_bearing + 45, grid_length)
        
                grid_square = {
                    'sw.lat': ll1.lat,
                    'sw.lng': ll1.lon,
                    'nw.lat': nw_ll.lat,
                    'nw.lng': nw_ll.lon,
                    'ne.lat': ll2.lat,
                    'ne.lng': ll2.lon,
                    'se.lat': se_ll.lat,
                    'se.lng': se_ll.lon,
                    'center.lat': ll_center.lat,
                    'center.lng': ll_center.lon
                    }
                yield grid_square
                ll1 = ll1.offset(self.diagonal_bearing + 45, grid_length) #move <grid_length> meters to the east
            ll_orig = ll_orig.offset(self.diagonal_bearing - 45, grid_length) #move starting point og grid walk <grid_length> meters north
            ll1 = ll_orig #reset lat1, lng1

    def temporal_grid(self,start_time,stop_time,timestep=3600):
        #input: a start and stop time and timestep (in seconds)
        #output: grid squares over space and time; compatible as ElasticSearch geo_point types
        walk = self.grid_walk()
        time_d = dt.timedelta(seconds=timestep)
        next_time = start_time
        out = []
        try:
            while True:
                points = walk.next()
                while next_time < stop_time:
                    grid = {'grid_time':next_time,
                            'grid_center':[float(points['center.lng']),float(points['center.lat'])],
                            'grid_boundary': {
                                            'type':'polygon',
                                            'coordinates':
                                                        [
                                                           [
                                                               [float(points['sw.lng']),float(points['sw.lat'])],
                                                               [float(points['se.lng']),float(points['se.lat'])],
                                                               [float(points['ne.lng']),float(points['ne.lat'])],
                                                               [float(points['nw.lng']),float(points['nw.lat'])],
                                                               [float(points['sw.lng']),float(points['sw.lat'])]
                                                            ]

                                                        ]
                                }
                            }
                            
                    next_time = next_time + time_d
                    out.append(grid)
                #reset and move to the next grid square
                next_time = start_time
        except StopIteration:
            pass
        return out

    def print_grid(self):
        #prints the grid walk
        walk = self.grid_walk()
        try:
            while True:
                print walk.next()
        except StopIteration:
            pass

    def grid_kml(self,filename='grid.kml',include_center=False):
        #creates a Google Earth KML file of the grid points
        walk = self.grid_walk()
        kml = simplekml.Kml()
        index = 0
        try:
            while True:
                index+=1
                points = walk.next()

                coords = [(points['sw.lng'],points['sw.lat'])]
                pnt = kml.newpoint(name='', coords=coords)
                pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'

                if include_center:
                    coords = [(points['center.lng'],points['center.lat'])]
                    pnt = kml.newpoint(name='', coords=coords)
                    pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'

                coords = [(points['ne.lng'],points['ne.lat'])]
                pnt = kml.newpoint(name='', coords=coords)
                pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'
        except StopIteration:
            pass
        kml.save(os.path.join(os.getcwd(),filename))


