import os
from pathlib import Path

import networkx as nx

from osm_network_types import OSMNetworkTypes
from osm_graph_generation import create_graph_from_osm


OSM_GRAPH_DATA_DIR = Path(os.environ['OSM_DATA_DIR'])


def create_city_topology_graphs(bbox: dict) -> None:
    g_walk = create_graph_from_osm(bbox, OSMNetworkTypes.WALK, speed=5)
    g_bike = create_graph_from_osm(bbox, OSMNetworkTypes.BIKE, speed=15)
    g_drive = create_graph_from_osm(bbox, OSMNetworkTypes.DRIVE, largest_component=True)

    nx.write_gpickle(g_drive, OSM_GRAPH_DATA_DIR.joinpath('ams_drive_network.gpickle'))
    nx.write_gpickle(g_bike, OSM_GRAPH_DATA_DIR.joinpath('ams_bike_network.gpickle'))
    nx.write_gpickle(g_walk, OSM_GRAPH_DATA_DIR.joinpath('ams_walk_network.gpickle'))

    nx.write_gml(g_drive, OSM_GRAPH_DATA_DIR.joinpath('ams_drive_network.gml'))
    nx.write_gml(g_bike, OSM_GRAPH_DATA_DIR.joinpath('ams_bike_network.gml'))
    nx.write_gml(g_walk, OSM_GRAPH_DATA_DIR.joinpath('ams_walk_network.gml'))
