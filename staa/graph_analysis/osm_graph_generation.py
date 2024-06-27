import osmnx as ox
from osm_network_types import OSMNetworkTypes
import networkx as nx


def create_graph_from_osm(bbox: dict, network_type: OSMNetworkTypes, largest_component: bool = False,
                          speed: float = None) -> nx.MultiDiGraph:
    g = ox.graph_from_bbox(
        bbox['north'],
        bbox['south'],
        bbox['east'],
        bbox['west'],
        network_type.value
    )

    if largest_component:
        g = ox.utils_graph.get_largest_component(g, strongly=True)

    if speed:
        nx.set_edge_attributes(g, speed, 'speed_kph')
    else:
        g = ox.speed.add_edge_speeds(g, precision=1)

    g = ox.speed.add_edge_travel_times(g, precision=1)

    return g
