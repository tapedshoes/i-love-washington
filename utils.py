# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 15:24:52 2020

@author: taped
"""

import os
from subprocess import Popen, PIPE
import numpy as np
import pandas as pd
import ujson as json

# WARNING! In order to get spatialite functions to work with sqlite3, you must have both the 
# spatialite DLLS and EXEs in their own folder, along with modified MINGW-64 DLLs
# First, download the latest stable mod_spatialite binaries from http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/
# The file you want is mod_spatialite-4.3.0a-win-amd64.7z (or whatever the current version is)
# Next, download the latest binaries for MINGW-W64. These will have the file name like x86_64-8.1.0-release-win32-seh-rt_v6-rev0.7z
# Ming64 releases can be found at https://sourceforge.net/projects/mingw-w64/files/mingw-w64/mingw-w64-release/
# Once both are downloaded and unzipped, delete libstdc++_64-6.dll and libgcc_s_seh_64-1.dll from the spatialite binaries
# Next, copy libstdc++-6.dll and libgcc_s_seh-1.dll from the ./mingw-w64/bin to the spatialite folder
# Then rename libstdc++-6.dll and libgcc_s_seh-1.dll to libstdc++_64-6.dll and libgcc_s_seh_64-1.dll
# Finally, set the path to the spatialite folder as the first item in your system path (see example below). 
# You can now deleted the mingw-w64 folder.
# Inspirations:
    # https://stackoverflow.com/questions/1556436/sqlite-spatialite-problems
    # http://blog.jrg.com.br/2016/04/25/Fixing-spatialite-loading-problem/
spatialite_folder = r'E:\GeospatialData\Projects\i-love-washington\data\mod_spatialite-4.3.0a-win-amd64'
os.environ['PATH'] = spatialite_folder + ';' + os.environ['PATH']
import sqlite3



# WARNING! In order for shapely to load successfully in a virtual environment on a machine that
# already has anaconda installed, the CONDA_PREFIX environment variable must be removed. 
# Shapely will attempt to search your CONDA install directory for shapely first. This can 
# lead to version incompatibilities
if 'CONDA_PREFIX' in os.environ:
    del os.environ['CONDA_PREFIX']
    
from shapely.wkt import dumps, loads
from shapely.geometry import mapping

# WARNING! Anything rasterio must be imported AFTER shapely or else YOU WILL DIE
from rasterio.io import MemoryFile
from rasterio import Affine

gdal_path = r'C:\Miniconda3\envs\hlz\Lib\site-packages\osgeo'

geom_types = [
    'POINT', 
    'LINESTRING', 
    'POLYGON', 
    'MULTIPOINT', 
    'MULTILINESTRING', 
    'MULTIPOLYGON', 
    'GEOMETRYCOLLECTION']


def ogr2pandas(dataset, layer=None, columns=[], where=None, sql=None):
    '''Use OGR to read a dataset into pandas'''
    
    
    ogrinfo = os.path.join(gdal_path, 'ogrinfo')
    
    if not columns:
        columns = ['*']
    
    if layer:
        table = layer
    else:
        layer = os.path.basename(dataset).split('.')[0]
        
        
    if not sql:
        sql = f'SELECT {",".join(columns)} FROM {table}'
        
        if where:
            sql += f' WHERE {where}'
    
    args = [
        ogrinfo,
        '-ro','-q',
        '-dialect', 'SQLite', 
        '-sql',  sql,
        dataset
        ]
    
    process = Popen(args, stdout=PIPE, stderr=PIPE)
    
    data = []
    
    row, last_line = {}, None
    while True:
        
        line = process.stdout.readline().decode()
        
        if line == '' and process.poll() is not None:
            break
        
        line = line.strip()
        # print(line)
        
        if 'OGRFeature' in line:
            if row != {}:
                data.append(row)
            row = {}
            continue
        
        if not line and last_line and last_line.split(' ')[0].upper() in geom_types:
            row['geom'] = loads(last_line)
            
        
        if ' = ' in line:
            row[line.split(' ')[0]] = line.split(' = ')[-1].strip()
        
        last_line = line
        
        
    if row != {}:
        data.append(row)
        
            
    print(process.stderr.read().decode())
        
    df = pd.DataFrame(data)
    
    return df
 
   
def gdalbytes2numpy(data, return_memfile=False):
    '''Convert the stdout of gdal to numpy array and metadata'''
    
    if return_memfile:
        return MemoryFile(data)

    with MemoryFile(data) as memfile:
        
        with memfile.open() as dataset:
            
            data_array = dataset.read()
            
            profile = dataset.profile
            
    return data_array, profile


def numpy2bytes(data_array, profile):
    '''
    Parameters
    ----------
    data_array : numpy.ndarray
        Numpy array
    profile : rasterio.profiles.Profile
        Rasterio profile

    Returns
    -------
    out_bytes : bytes or rasterio.io.MemoryFile
        DESCRIPTION.

    '''
    
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(data_array)
            del data_array
        
        out_bytes = memfile.read()
        return out_bytes

def df2geojson(df, geom):
    data = {
        "type": "FeatureCollection",
        "features": []}
    
    for row in df.to_dict('records'):
        # Extract the geometry
        row_geom = row.pop(geom)
        
        # Check for valid datatypes and correct
        row = {k: row[k] if type(row[k]) in [str, int, float] else str(row[k]) for k in row}
        
        # Construct the geojson structure
        row = {
            'type': 'Feature',
            'properties': row,
            'geometry': mapping(row_geom)}
        
        # Append to the output list of features
        data['features'].append(row)
    
    j = json.dumps(data)
        
    return j

def gdalinfo(raster):
    in_format = raster
    if type(raster) == bytes:
        in_format = '/vsistdin/'
    
    
    args = [
        os.path.join(gdal_path, 'gdalinfo'),
        '-json', 
        in_format]
    
    
    if type(raster) == bytes:
        process = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate(input=raster)
    else:
        process = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        
    print(stderr.decode())
    
    j = json.loads(stdout.decode())
    return j

def array2coords(array, array_transform, nodata):
    '''
    

    Parameters
    ----------
    array : numpy.ndarray
        2D Numpy array to be converted
    array_transform : affine.Affine
        The affine transformation needed. Can be derived from a rasterio.profile
    nodata : float
        Value to be ignored from output dataset

    Returns
    -------
    pandas.DataFrame

    '''

    T1 = array_transform * Affine.translation(0.5, 0.5) # reference the pixel centre
    rc2xy = lambda r, c: (c, r) * T1  
    row, col = np.where(array != nodata)
    z = np.extract(array != nodata, array)
    
    df_coords = pd.DataFrame({'col':col,'row':row,'z':z})
    df_coords['x'] = df_coords.apply(lambda row: rc2xy(row.row,row.col)[0], axis=1)
    df_coords['y'] = df_coords.apply(lambda row: rc2xy(row.row,row.col)[1], axis=1)
    
    return df_coords

def connect_spatialite(path):

    conn = sqlite3.connect(path)
    conn.enable_load_extension(True)
    conn.load_extension('mod_spatialite')
    cur = conn.cursor()
    
    return conn, cur

def reproject_array_to_file(array, profile, dst_crs, save_file):
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    with MemoryFile() as memfile:
        with memfile.open(**profile) as src:
            src.write(array)
                
        with memfile.open() as src:

            transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
    
            with rasterio.open(save_file, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest)
    return True