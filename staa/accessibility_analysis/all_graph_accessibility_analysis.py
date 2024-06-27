import numpy as np
import os
from pathlib import Path
import igraph as ig
import geopandas as gpd
from multiprocessing.pool import ThreadPool
from sklearn.neighbors import BallTree
import math
import logging
import pickle

logging.basicConfig()
logger = logging.getLogger("graph_accessibility_analysis")
logger.setLevel(logging.INFO)

EARTH_RADIUS_M = 6_371_009

GRAPH_DATA_DIR = Path(os.environ["GRAPH_DATA_DIR"])
OPPORTUNITIES_GEO_JSON = Path(os.environ["OPPORTUNITIES_GEO_JSON"])
NEIGHBOURHOODS_GEO_JSON = Path(os.environ["NEIGHBOURHOODS_GEO_JSON"])
RESULTS_PATH = Path(os.environ["RESULTS_PATH"])
POI_TYPE_NAME = os.environ["POI_TYPE_NAME"]
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 2))

# Global DataFrames
destinations = gpd.read_file(OPPORTUNITIES_GEO_JSON)
destinations.geometry = gpd.points_from_xy(destinations.geometry.y, destinations.geometry.x, crs='EPSG:4326')

poi_gdf = destinations[destinations.Functie == POI_TYPE_NAME]
poi_gdf.geometry = gpd.points_from_xy(poi_gdf.geometry.x, poi_gdf.geometry.y, crs='EPSG:4326')

# Read Amsterdam Neighborhoods
nb_gdf = gpd.read_file(NEIGHBOURHOODS_GEO_JSON)
nb_gdf['centroid'] = gpd.points_from_xy(nb_gdf.cent_x, nb_gdf.cent_y, crs='EPSG:4326')
nb_gdf['res_centroid'] = gpd.points_from_xy(nb_gdf.res_cent_x, nb_gdf.res_cent_y, crs='EPSG:4326')
# Places without residential buildings have no residential centroids.
# Find them and assign to them the geographical centroid.
nb_gdf.loc[nb_gdf['res_cent_x'].isna(), 'res_centroid'] = nb_gdf[nb_gdf['res_cent_x'].isna()]['centroid']


def nearest_nodes_to_points(G, X, Y, return_dist=False):
    """OSMNX nearest_nodes function adapted to igraph
    from https://github.com/gboeing/osmnx/blob/main/osmnx/distance.py: nearest_nodes
    For a given set of geographical locations (X, Y), return the nearest nodes of the given graph G.
    Args:
        G (igraph.Graph): input graph
        X (pandas.Series): X coordinates of the input points
        Y (pandas.Series): Y coordinages of the input points
        return_dist (bool, optional): If True the distance to the nearest node for all points is returned. Defaults to False.
    Returns:
        list: list of nodes of graph G
    """
    if np.isnan(X).any() or np.isnan(Y).any():  # pragma: no cover
        raise ValueError("`X` and `Y` cannot contain nulls")

    nodes = np.array([[node['y'], node['x']] for node in G.vs])

    nodes_rad = np.deg2rad(nodes)
    points_rad = np.deg2rad(np.array([Y, X]).T)

    dist, pos = BallTree(nodes_rad, metric='haversine').query(points_rad, k=1)
    dist = dist[:, 0] * EARTH_RADIUS_M  # convert radians -> meters

    nn = G.vs[pos[:, 0].tolist()]
    nn = list(nn)
    dist = dist.tolist()

    if return_dist:
        return nn, dist
    else:
        return nn


def run_analysis(graph_path: Path):
    # Read the transit network
    G_transit = ig.read(graph_path)
    # For each neighborhood, get its nearest node in the network.
    nb_nodes, nb_dist = nearest_nodes_to_points(G_transit, nb_gdf['res_centroid'].x, nb_gdf['res_centroid'].y,
                                                return_dist=True)
    # For each POI, get its nearest node in the network.
    poi_nodes, poi_dist = nearest_nodes_to_points(G_transit, poi_gdf['geometry'].x, poi_gdf['geometry'].y,
                                                  return_dist=True)

    logger.info(f"Processing graph {graph_path.with_suffix('').name} and have the following statistics:\n"
                f"Average point to node distance: {np.average(nb_dist)} "
                f"ranging from [{np.min(nb_dist)},[{np.max(nb_dist)}]]\n"
                f"Average POI to node distance: {np.average(poi_dist)} "
                f"ranging from [{np.min(poi_dist)},[{np.max(poi_dist)}]]")

    # Calculate travel times between all neighborhoods and all POIs.
    # tt_mx.shape = (nr of neighborhoods (origins), nr of POIs (destinations))
    tt_mx = np.ndarray((len(nb_nodes), len(poi_nodes)))
    td_mx = np.ndarray((len(nb_nodes), len(poi_nodes)))
    modes_mx = np.ndarray((len(nb_nodes), len(poi_nodes)))
    lines_mx = np.ndarray((len(nb_nodes), len(poi_nodes)))
    hops_mx = np.ndarray((len(nb_nodes), len(poi_nodes)))

    failed = {}

    for i, o in enumerate(nb_nodes):
        if i % 100 == 0:
            logger.info(f"Processing graph {graph_path} origin node {i}")
        for j, d in enumerate(poi_nodes):
            # Travel Time
            tt = G_transit.shortest_paths(o, d, weights='travel_time')[0][0]
            if tt == math.inf:
                if not failed.get(f"{o['node_id']}_tt", None) == d['node_id']:
                    # logger.warning(f'Failed to get TRAVEL TIME for {o["node_id"]} - {d["node_id"]}: '
                    #                f'No path found between them')
                    failed[f"{o['node_id']}_tt"] = d["node_id"]
            else:
                tt_mx[i, j] = tt + poi_dist[j] + nb_dist[i]
            # Travel Distance
            td = G_transit.shortest_paths(o, d, weights='length')[0][0]
            if td == math.inf:
                if not failed.get(f"{o['node_id']}_td", None) == d["node_id"]:
                    # logger.warning(f'Failed to get TRAVEL DISTANCE for {o["node_id"]} - {d["node_id"]}: '
                    #                f'No path found between them')
                    failed[f"{o['node_id']}_td"] = d["node_id"]
            else:
                td_mx[i, j] = td + poi_dist[j] + nb_dist[i]
            # Number of hops and number of modes
            edges = G_transit.get_shortest_paths(o, d, weights='length', output='epath')[0]
            if edges == math.inf or not edges:
                if not failed.get(f"{o['node_id']}_edges", None) == d["node_id"]:
                    # logger.warning(f'Failed to get EDGES for {o["node_id"]} - {d["node_id"]}: '
                    #                f'No path found between them')
                    failed[f"{o['node_id']}_edges"] = d["node_id"]
            else:
                n_modes = len(np.unique(G_transit.es[edges]['route_type']))
                n_lines = len(np.unique(G_transit.es[edges]['unique_route_id']))
                # Add walking if there is some
                modes_mx[i, j] = n_modes + int(poi_dist[j] > 0 or nb_dist[i] > 0)
                lines_mx[i, j] = n_lines
                # Add walking if there is some
                hops_mx[i, j] = len(edges) + int(poi_dist[j] > 0) + int(nb_dist[i] > 0)

    od_mat_path = RESULTS_PATH.joinpath(f"{Path(graph_path).with_suffix('').name}_computation.pkl")
    logger.info(f"Finished processing graph {graph_path.with_suffix('').name} storing it in path: {od_mat_path}")

    with open(od_mat_path, "wb") as fp:
        pickle.dump([tt_mx, td_mx, modes_mx, lines_mx, hops_mx, failed], fp)

    return od_mat_path


if __name__ == "__main__":
    graph_folders = [d for d in os.listdir(GRAPH_DATA_DIR) if os.path.isdir(GRAPH_DATA_DIR.joinpath(d))]
    graphs = [GRAPH_DATA_DIR.joinpath(folder).joinpath(file) for folder in graph_folders for file in
              os.listdir(GRAPH_DATA_DIR.joinpath(folder)) if Path(file).suffix == '.gml']

    results = ThreadPool(NUM_WORKERS).imap(run_analysis, graphs)

    generated_paths = []

    for r in results:
        generated_paths.append(r)

    logger.info(f"Generated {len(generated_paths)} OD matrix tuples in {generated_paths}")
