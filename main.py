# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 16:48:43 2020

@author: taped
"""


import utils, osm, landsat

import logging
logging.basicConfig(format='%(asctime)s %(message)s')
logging.warning('is when this event was logged.')
import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling

def main(sqlite, state, folder):
    
    import pyproj
    from shapely.ops import transform
    from shapely.geometry import mapping
    from shapely.prepared import prep
    from rasterio.mask import mask
    from rasterio.enums import Resampling
    
    sqlite = r'E:\GeospatialData\Projects\i-love-washington\data\data.sqlite'
    state = 'Washington'
    folder = "E:\GeospatialData\Projects\i-love-washington\data\mosaic"
    
    # Create the output database table
    conn, cur = utils.connect_spatialite(sqlite)
    cur.execute('DROP TABLE lake_points')
    
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS lake_points (
            oid INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
            z DOUBLE,
            path_ INTEGER,
            row_ INTEGER)
        ''')
            
    cur.execute("SELECT AddGeometryColumn('lake_points', 'geom',3857, 'POINT', 2)") 
    cur.execute("DELETE FROM lake_points") 
    conn.commit()
    cur.close(), conn.close()
    
    sql='''
        SELECT name, TRANSFORM(geometry, 3857) geom
        FROM multipolygons 
        WHERE boundary='administrative' AND name='Washington' 
        '''
    df_wa = utils.ogr2pandas( sqlite, sql=sql)
    wa_geom = df_wa.iloc[0].geom
    wa_buf = wa_geom.buffer(500, 1)
    bounds = wa_buf.bounds
    
    # Pull lakes from database
    lakes_sql = '''
        SELECT name, ST_TRANSFORM(ST_UNION(geometry), 3857) geom, ST_AREA(ST_TRANSFORM(ST_UNION(geometry), 3857)) area
        FROM multipolygons 
        WHERE natural='water' 
        AND name IS NOT NULL 
        AND ST_AREA(geometry) > 0.0001 
        AND (LOWER(name) LIKE '%lake%' OR LOWER(name) LIKE '%pool%' OR LOWER(name) LIKE '%reservoir%') 
        GROUP BY name 
        ORDER BY ST_AREA(ST_BUFFER(geometry, -0.001, 10)) DESC 
        LIMIT 100
        '''
    df_lakes = utils.ogr2pandas( sqlite, sql=lakes_sql )
    
    # Apply a negative buffer, used for ignoring shoreline temperatures when sampling raster
    df_lakes['neg_buf'] = df_lakes['geom'].apply(lambda x: x.buffer(-150, 2) )
    
    # Prepare lakes for efficient lookups once
    
    df_lakes['prep'] = df_lakes['geom'].apply(lambda x: prep(x))
    
    # print('Fetching LANDSAT WRS geoms')
    # df_wrs = utils.ogr2pandas(
    #     sqlite, 
    #     'WRS2', 
    #     columns=['PATH', 'ROW', 'geometry'])
    
    df_wrs = utils.ogr2pandas(
        dataset = "E:\GeospatialData\Projects\i-love-washington\data\data.sqlite", 
        sql="""
            SELECT PATH, ROW, w.geometry 
            FROM multipolygons m, wrs2 w 
            WHERE m.name = 'Washington' 
            AND ST_INTERSECTS(m.geometry, w.geometry)
            """
            )
    
    for row in df_wrs.to_dict('records'):
        logging.warning(f'Starting row {row["row"]}, path {row["path"]}')
        

        # Identify a cloudless scene
        mtl_md = landsat._return_candidates(int(row['path']), int(row['row']), 8, 5)
        
        logging.warning(f'Identified {len(mtl_md)} scenes')
        
        for key in mtl_md:
            metadata = mtl_md[key]
            
            save_file = utils.os.path.join(folder, f"{metadata['LANDSAT_PRODUCT_ID']}_FARENHEIT.TIF")
            if utils.os.path.exists(save_file):
                logging.warning('    Already Exists')
                continue
            
            # Pull image data from google into the metadata key 'bytes'
            logging.warning('Downloading image')
            landsat_bytes = landsat._pull_raster(metadata, 10)
            
            # Read Image
            logging.warning('Reading bytes to array')
            array, profile = utils.gdalbytes2numpy(landsat_bytes)
            
            logging.warning('Converting to farenheit')
            farenheit = landsat.array2farenheit(array, metadata, -99999).round(1)
            
            # Set profile nodata to -99999 and output type to float32
            profile['nodata'] = -99999
            profile['dtype'] = 'float32'
            
            # Reproject image bytes folder
            logging.warning('Writing to file')
            save_file = utils.os.path.join(folder, f"{metadata['LANDSAT_PRODUCT_ID']}_FARENHEIT.TIF")
            utils.reproject_array_to_file(farenheit, profile, 'EPSG:3857', save_file)
            j = utils.json.dumps(metadata, indent=2)
            with open(save_file.replace('FARENHEIT.TIF', 'METADATA.json'), 'w') as w:
                w.write(j)
                
    
    tifs = [rasterio.open(utils.os.path.join(folder, f)) for f in utils.os.listdir(folder) if '_T1_FARENHEIT.TIF' in f]   
    bounds = (-13897124.13181157, 5707029.565730883, -13014721.44634677, 6275775.285325819)
    import numpy as np
    def calc_median(old_data, new_data, old_nodata, new_nodata, **kwargs):
        mask = np.logical_and(~old_nodata, ~new_nodata)
        old_data[mask] = np.ma.median([old_data[mask], new_data[mask]], axis=0)
    
        mask = np.logical_and(old_nodata, ~new_nodata)
        old_data[mask] = new_data[mask]
    
    mosaic, out_trans = merge(
        datasets=tifs,
        bounds=bounds,
        precision=1,
        method=calc_median)
    
    [tif.close() for tif in tifs]
    
    out_meta = tifs[0].meta.copy()
    out_meta.update({"driver": "GTiff",
                     "height": mosaic.shape[1],
                     "width": mosaic.shape[2],
                     "transform": out_trans,
                     'compress':'LZW',
                     'tiled':True})
    out_fp = utils.os.path.join(folder, 'mosaic_median.tif')
    with rasterio.open(out_fp, "w", **out_meta) as dest:
        dest.write(mosaic)
        
    del mosaic
    
    mosaic = rasterio.open(out_fp)
    profile = mosaic.profile
    
    
    # Iterate through the lakes and export a PNG
    for lake in df_lakes.to_dict('records'):
        break
        geoj = mapping(lake['geom'])
    
        # Mask the landsat image to lake pixels
        array, array_transform = mask(src, [geoj], crop=True)
    
    
    
        
    # mosaic, out_trans = merge(
    #     datasets=tifs,
    #     bounds=bounds,
    #     precision=1,
    #     method='max')
    # out_meta = tifs[0].meta.copy()
    # out_meta.update({"driver": "GTiff",
    #                  "height": mosaic.shape[1],
    #                  "width": mosaic.shape[2],
    #                  "transform": out_trans,
    #                  'compress':'LZW',
    #                  'tiled':True})
    # out_fp = utils.os.path.join(folder, 'mosaic_max.tif')
    # with rasterio.open(out_fp, "w", **out_meta) as dest:
    #     dest.write(mosaic)
    
    
            
    # # Write VRT
    # tifs = [utils.os.path.join(folder, f) for f in utils.os.listdir(folder) if f[-4:].lower() == '.tif']
    # args = [
    #     utils.os.path.join(utils.gdal_path, 'gdalbuildvrt'),
    #     '-hidenodata', '-srcnodata', '-99999',
    #     '-r', 'average',
    #     utils.os.path.join(folder, 'mosaic.vrt')]
    # args.extend(tifs)
    # p = utils.Popen(args, stdout=utils.PIPE, stderr=utils.PIPE)
    # stdout, stderr = p.communicate()

    # # Write mosaic
    
    # args = [
    #     utils.os.path.join(utils.gdal_path, 'gdalwarp'),
    #     '-overwrite',
    #     '--config', 'GDAL_CACHEMAX', '512',
    #     '-srcnodata', '-99999',
    #     '-co', 'TILED=YES',
    #     '-co', 'COMPRESS=LZW',
    #     '-r', 'med',
    #     '-wm', '2000',
    #     utils.os.path.join(folder, 'mosaic.vrt'),
    #     utils.os.path.join(folder, 'mosaic.tif'),
        
    #     ]
    
    # p = utils.Popen(args, stdout=utils.PIPE, stderr=utils.PIPE)
    # stdout, stderr = p.communicate()
        
        
        
        
        
        
    # # Get the projection of the image
    # logging.warning('Identifying projection')
    # metadata.update(utils.gdalinfo(metadata['bytes']))
    # landsat_crs = pyproj.CRS(metadata['coordinateSystem']['wkt'])
    # wgs84 = pyproj.CRS('EPSG:4326')
    # project = pyproj.Transformer.from_crs(wgs84, landsat_crs, always_xy=True).transform
    
    # # Filter geometries to those that intersect with the scene, project to the correct UTM, and convert to geojson
    # logging.warning('Filtering lakes to those in the scene')
    # geoms = [mapping(transform(project, x)) for x in df_lakes['neg_buf'] if x.intersects(row['geom'])]
     
    # logging.warning('Creating raster memfile from image bytes')
    # memfile = utils.gdalbytes2numpy(metadata['bytes'], return_memfile=True)
    

    # logging.warning('Masking raster by lakes')
    # with memfile.open() as src:
    #     # Get the metadata of the landsat image
    #     profile = src.profile
        
    #     # Mask the landsat image to lake pixels
    #     array, array_transform = mask(src, geoms, crop=True)
        
    # # Set profile nodata to -99999 and output type to float32
    # profile['nodata'] = 0
        
    #     profile['nodata'] = -99999
    #     profile['dtype'] = 'float32'
        
    #     # Create coordinates from valid data
    #     logging.warning('Creating coordinates')
    #     df_coords = utils.array2coords(farenheit[0], array_transform, -99999)
    #     df_coords['row_'] = row['row']
    #     df_coords['path_'] = row['path']
        
    #     # Write coords to database
    #     logging.warning('Writing coordinates to database')
    #     conn, cur = utils.connect_spatialite(sqlite)
    #     cur.executemany(
    #         f'INSERT INTO lake_points (geom, z, row_, path_) VALUES ( Transform(MakePoint(?, ?, {landsat_crs.to_epsg()}), 3857), ?, ?, ?)',
    #         df_coords[['x', 'y', 'z', 'row_', 'path_']].itertuples(index=False) ) 
    #     conn.commit()
    #     cur.close(), conn.close()
        
    
    # Move to top
    # # Create a copy of the lakes in the database for editing
    # args = [
    #     utils.os.path.join(utils.gdal_path, 'ogr2ogr'),
    #     '-append',
    #     '-t_srs', 'EPSG:3857',
    #     '-sql', lakes_sql,
    #     sqlite, sqlite, 'multipolygons',
    #     '-nln','lakes_3857']
    # p = utils.Popen(args, stdout=utils.PIPE, stderr=utils.PIPE)
    # stdout, stderr = p.communicate()
    
    # args[-1] = 'lakes_3857_modified'
    # p = utils.Popen(args, stdout=utils.PIPE, stderr=utils.PIPE)
    # stdout, stderr = p.communicate()
    
    
    conn, cur = utils.connect_spatialite(sqlite)
    cur.execute("SELECT CreateSpatialIndex('lake_points', 'geom')")
    