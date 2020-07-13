# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 11:51:36 2020

@author: taped
"""

import os
from subprocess import Popen, PIPE

_osmconf = None
gdal_path = r'C:\Miniconda3\envs\hlz\Lib\site-packages\osgeo'


def osmpbf_2_gpkg(in_pbf, out_sqlite):
    '''
    Convert an OSM PBF to sqlite

    Parameters
    ----------
    in_pbf : string
        Local file path for the osm pbf file
    out_sqlite : string
        Local file path for newly created spatialite sqlite. Should end in .gpkg

    Returns
    -------
    out_sqlite
    '''
    
    ogr2ogr_path = os.path.join(gdal_path, 'ogr2ogr')
    osmconf  = _osmconf
    
    # attempt to find the osmconf file
    if not osmconf:
        osmconf_locations = [
            [gdal_path, 'data', 'gdal'], # GDAL compiled from wheel
            [os.path.dirname(gdal_path), 'share', 'gdal'], # GDAL installed from conda or osgeo4w
            [os.path.dirname(gdal_path), 'gdal-data'], # GDAL in postgres
            ]
        
        for location in osmconf_locations:
            try:
                osmconf = os.path.join(location[0], location[1], location[2], 'osmconf.ini')
                if os.path.exists(osmconf):
                    break
            except:
                pass
            
        if not osmconf:
            raise Exception('No OSM conf file found, either set it at the class level or download one')

    
    for layer in ['points', 'lines', 'multipolygons', 'multilinestrings']:
        print(layer)
    
        args = [
            ogr2ogr_path, 
            '-f', 'SQLite', 
            '-append',
            '--config','OSM_MAX_TMPFILE_SIZE', '2000',
            '--config', 'OSM_COMPRESS_NODES', 'YES',
            '--config', 'OGR_SQLITE_SYNCHRONOUS', 'OFF',
            '--config', 'OSM_CONFIG_FILE', osmconf,
            out_sqlite,
            in_pbf, layer, 
            # '-lco', 'spatial_index=no', 
            '-gt',  '65536', 
            '-nln', layer,
            '-dsco','SPATIALITE=YES'
            ]
        
        process = Popen(args, stderr=PIPE, stdout=PIPE)
        
        stdout, stderr = process.communicate()
        
        if stderr:
            print(stderr.decode())
            
    return out_sqlite
            
            
 

    
#https://download.geofabrik.de/north-america/us/washington-latest.osm.pbf

# Open a zipped shapefile on the web
# ogrinfo -ro -al -so /vsizip//vsicurl/https://prd-wret.s3-us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/atoms/files/WRS2_descending_0.zip --config GDAL_HTTP_UNSAFESSL YES
