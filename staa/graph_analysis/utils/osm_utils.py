import osmnx as ox


def get_bbox(city: str) -> dict:
    gdf = ox.geocode_to_gdf({'city': city})
    return dict(
        west=gdf.loc[0, 'bbox_west'],
        south=gdf.loc[0, 'bbox_south'],
        east=gdf.loc[0, 'bbox_east'],
        north=gdf.loc[0, 'bbox_north']
    )
