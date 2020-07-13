# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 11:08:58 2020

@author: taped
"""


import os
from subprocess import Popen, PIPE
import numpy as np
import re


gdal_path = r'C:\Miniconda3\envs\hlz\Lib\site-packages\osgeo'
if 'CONDA_PREFIX' in os.environ:
    del os.environ['CONDA_PREFIX']

from shapely.ops import cascaded_union
from shapely import wkt, geometry
from rtree import index





# Google needs to have application credentials set in order to request imagery
# see details at https://cloud.google.com/storage/docs/reference/libraries#windows
from google.cloud import storage
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = input('Google Application Credentials JSON Path: ')
client = storage.Client()
bucket = client.get_bucket('gcp-public-data-landsat')

def landsat8_2_radiance(band, a, m):
    '''
    Convert landsat 8 band to TOA Radiance

    Parameters
    ----------
    band : numpy.ndarray
        Quantized and calibrated standard product pixel values (DN)
    a : float
        Band-specific additive rescaling factor from the metadata 
        (RADIANCE_ADD_BAND_x, where x is the band number)
    m : float
        Band-specific multiplicative rescaling factor from the metadata 
        (RADIANCE_MULT_BAND_x, where x is the band number)

    Returns
    -------
    numpy.ndarray : TOA spectral radiance (Watts/( m2 * srad * μm))

    '''
    
    rad = (band * m) + a
    
    return rad

def landsat8b10_2_temp(b10_rad, k1, k2):
    '''
    Convert landsat thermal band10 from DN units to Celcius units
    
    Formula source: https://www.usgs.gov/land-resources/nli/landsat/using-usgs-landsat-level-1-data-product
    Arcpy Formated Code inspiration: https://github.com/NASA-DEVELOP/dnppy/blob/master/dnppy/landsat/surface_temp.py
    
    Parameters
    ----------
    b10_rad : numpy.ndarray 
        Band 10 TOA spectral radiance (Watts/( m2 * srad * μm))
    k1 : float
        Band-specific thermal conversion constant from the metadata 
        (K1_CONSTANT_BAND_x, where x is the thermal band number)
    k2 : float
        Band-specific thermal conversion constant from the metadata 
        (K2_CONSTANT_BAND_x, where x is the thermal band number)

    Returns
    -------
    numpy.ndarray : Top of atmosphere brightness temperature (K)
    
    '''
    
    temp = (k2 / (np.log ( (k1 / b10_rad) + 1) ) )
    
    return temp
    
def kelvin2farenheit(k):
    'Convert kelvin to farenheit'
    return ( (k - 273.15) * (9 / 5) ) + 32 


def kelvin2celcius(k):
    'Convert kelvin to celcius'
    return k - 273.15

def wrs2sqlite(sqlite):
    '''
    Download the landsat WRS to spatialite sqlite database

    Parameters
    ----------
    sqlite : string
        File path to local sqlite

    Returns
    -------
    sqlite

    '''
    ogr2ogr_path = os.path.join(gdal_path, 'ogr2ogr')
    args = [
        ogr2ogr_path,
        '-overwrite',
        '--config', 'GDAL_HTTP_UNSAFESSL', 'YES',
        sqlite,
        '/vsizip//vsicurl/https://prd-wret.s3-us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/atoms/files/WRS2_descending_0.zip', 
        '-nln', 'WRS2',
        '-nlt', 'PROMOTE_TO_MULTI']
    
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    
    stdout, stderr = process.communicate()
        
    if stderr:
        print(stderr.decode())
            
    return sqlite



# def _get_path_rows_from_geom(geom, wrsdf):
#     wrs2_state = wrsdf[wrsdf.geom.apply(lambda x: x.intersects(geom) and geom.contains(x.centroid))]
    
#     return wrs2_state

def _find_first_cloudless(path, row, month):

    # https://cloud.google.com/storage/docs/public-datasets/landsat
    # https://googleapis.dev/python/storage/latest/buckets.html
    
    # Filter the list of files to MTL.txt files for a specific path, row, and month
    list_mtl = sorted([
        b.name for b in bucket.list_blobs(prefix = 'LC08/01/{:03d}/{:03d}'.format(path, row)) # /[SENSOR_ID]/01/[PATH]/[ROW]/[SCENE_ID]/
        if 'T1' in b.name 
        and 'MTL.txt' in b.name
        and re.search('20\d\d{:02d}\d\d'.format(month), b.name)], reverse=True) 
    
    mtl_md = {}

    for mtl in list_mtl:
        print(mtl)
        mtl_txt = bucket.get_blob(mtl).download_as_string()
        metadata = dict([line.strip().split(' = ') for line in mtl_txt.decode().split('\n') if ' = ' in line])
        
        metadata = {k: eval(metadata[k].lstrip('0')) if '"' in metadata[k]  or metadata[k].replace('.','',1).isdigit() else metadata[k] for k in metadata}
        
        if not re.search('-{:02d}-'.format(month), metadata['DATE_ACQUIRED']):
            continue

        mtl_md[mtl] = metadata
        
        if metadata['CLOUD_COVER'] <= 1:
            print('yep')
            return metadata
        else:
            print('nope')
            
    print('Nothing with less than cloud cover 1, taking scene with the lowest cloud cover')
    
    sorted_by_clouds = sorted( mtl_md.items(), key=lambda k_v: k_v[1]['CLOUD_COVER'])
    
    return sorted_by_clouds[0][1]

def _pull_raster(metadata, band):
    blob_name = f"LC08/01/{metadata['WRS_PATH']:0>3}/{metadata['WRS_ROW']:0>3}/{metadata['LANDSAT_PRODUCT_ID']}/{metadata['LANDSAT_PRODUCT_ID']}_B{band}.TIF"
    tif_bytes = bucket.get_blob(blob_name).download_as_string()
    
    metadata['bytes'] = tif_bytes
