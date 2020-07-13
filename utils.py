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




gdal_path = r'C:\Miniconda3\envs\hlz\Lib\site-packages\osgeo'
if 'CONDA_PREFIX' in os.environ:
    del os.environ['CONDA_PREFIX']
    
from shapely.wkt import dumps, loads
from shapely.geometry import mapping


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
 
   
def gdalbytes2numpy(data):
    '''Convert the stdout of gdal to numpy array and metadata'''
    from rasterio.io import MemoryFile
    
    with MemoryFile(data) as memfile:
        
        with memfile.open() as dataset:
            
            data_array = dataset.read()
            
            metadata = {
                'bounds': dict(dataset.bounds._asdict()),
                'profile': dataset.profile
                }
            
    return data_array, metadata
    
def numpy2bytes(data_array, profile):
    from rasterio.io import MemoryFile
    
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(data_array)
            
        out_bytes = memfile.read()
        
    return out_bytes

def df2json(df, geom):
    data = {
        "type": "FeatureCollection",
        "features": []}
    
    for row in df.to_dict('records'):
        row_geom = row.pop(geom)
        row = {
            'type': 'Feature',
            'properties': row,
            'geometry': mapping(row_geom)}
        data['features'].append(row)
    
    j = json.dumps(data)
        
    return j

# clip_raster()
df_lakes = ogr2pandas(
    "E:\GeospatialData\Projects\i-love-washington\data\data.sqlite", 
    sql='''
    SELECT name, geometry 
    FROM multipolygons 
    WHERE natural='water'
    AND name IS NOT NULL
    AND ST_Area(geometry) > 0.0001
    ORDER BY ST_Area(geometry) DESC
    LIMIT 100
    '''
    )

j = df2json(df_lakes, 'geom')
