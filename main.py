# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 16:48:43 2020

@author: taped
"""

import utils, osm, landsat



def main(sqlite, state):
    
    sqlite = r'E:\GeospatialData\Projects\i-love-washington\data\data.sqlite'
    state = 'Washington'
    
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
    
    print('Fetching state boundaries')
    geom_state = utils.ogr2pandas(
        sqlite, 
        'multipolygons', 
        columns=['geometry'], 
        where=f"boundary='administrative' AND name='{state}'").iloc[0].geom
    
    # print('Fetching LANDSAT WRS geoms')
    # df_wrs = utils.ogr2pandas(
    #     sqlite, 
    #     'WRS2', 
    #     columns=['PATH', 'ROW', 'geometry'])
    
    df_wrs = ogr2pandas(
        dataset = "E:\GeospatialData\Projects\i-love-washington\data\data.sqlite", 
        sql="""
            SELECT PATH, ROW 
            FROM multipolygons m, wrs2 w 
            WHERE m.name = 'Washington' 
            AND ST_INTERSECTS(m.geometry, ST_CENTROID(w.geometry))
            """
            )
    
    for row in df_wrs.to_dict('records'):
        
        break
    

        metadata = landsat._find_first_cloudless(int(row['path']), int(row['row']), 8)
        
        landsat._pull_raster(metadata, 10)
        
        
        