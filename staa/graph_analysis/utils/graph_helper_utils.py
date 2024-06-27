# From Dimitris Michealidis' Peoject-A
# https://github.com/dimichai

from haversine import haversine
import time

import numpy as np
import pandas as pd
import osmnx as ox

import networkx as nx

from .speeds import MetricTravelSpeeds
import logging


logger = logging.getLogger(__file__)


def append_length_attribute(ua_network: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """ UrbanAccess networks do not have a 'length' attribute (they have 'weight' instead.)
    We want to add the length attribute because it is needed for some osmnx methods.
    We will set it to the same value as the weight.
    Args:
        ua_network (urbanaccess.network.urbanaccess_network):

    Returns:
        urbanaccess.network.urbanaccess_network: [description]
    """
    ua_network_weights = {}
    for node1, node2, key, data in ua_network.edges(data=True, keys=True):
        ua_network_weights[(node1, node2, key)] = data['weight']

    nx.set_edge_attributes(ua_network, ua_network_weights, "length")
    nx.set_edge_attributes(ua_network, ua_network_weights, "travel_time")

    return ua_network


def append_hourly_stop_frequency_attribute(ua_network: nx.MultiDiGraph, hourly_stop_frequency_df: pd.DataFrame):
    """
    This function adds the 'hourly_frequency' attribute from the hourly_stop_frequency_df
    to a UrbanAccess network
    :param ua_network:
    :param hourly_stop_frequency_df:
    :return:
    """
    ua_network_stop_frequencies = {}
    for node, _ in ua_network.nodes(data=True):
        try:
            data = hourly_stop_frequency_df.loc[node, :]
            data_dict = data.to_dict()
            ua_network_stop_frequencies[node] = data_dict
        except KeyError as e:
            logger.warning(f"couldn't identify {node} in stop_ids")

    nx.set_node_attributes(ua_network, ua_network_stop_frequencies, "stop_frequencies")

    return ua_network


def append_hourly_edge_frequency_attribute(ua_network: nx.MultiDiGraph, hourly_leg_frequency_df: pd.DataFrame):
    """

    :param ua_network:
    :param hourly_leg_frequency_df:
    :return:
    """
    ua_network_edge_frequencies = {}
    for node1, node2, key, data in ua_network.edges(data=True, keys=True):
        try:
            data = hourly_leg_frequency_df.loc[(node2, node1), :]
            data_dict = data.to_dict()
            ua_network_edge_frequencies[(node1, node2, key)] = data_dict
        except KeyError as e:
            logger.warning(f"couldn't identify {(node2, node1)} in (stop_id, provenance_stop_id) index")

    nx.set_edge_attributes(ua_network, ua_network_edge_frequencies, "segment_frequencies")

    return ua_network


def parse_nx_node(row, attr, idcol):
    """Parses nodes into nx-format"""
    attr = row[attr].copy()
    return (row[idcol], attr.to_dict())


def parse_nx_edge(row, attr_cols, fr, to):
    """Parses edges into nx-format"""
    idx = row.name
    attr = row[attr_cols].copy().to_dict()
    return (row[fr], row[to], idx, attr)


def ua_transit_network_to_nx(transit_net) -> nx.MultiDiGraph:
    """Convert an urbanaccess transit network to networkx.
    ua2nx needs a transit+walk network to work, thus here the function is adjusted for when
    only a tranist network is available.

    Args:
        transit_net (urbanaccess.network.urbanaccess_network): transit network to convert to networkx.
    """
    nodes = transit_net.transit_nodes
    edges = transit_net.transit_edges
    graph = nx.MultiDiGraph()

    # Reset index for nodes to get the integer version of the node_id
    nodes = nodes.reset_index()
    fr = 'node_id_from'
    to = 'node_id_to'
    nodeid = 'node_id'

    # Don't make an edge out of every trip, instead aggregate them using the median travel time.
    edges = edges.groupby([fr, to])['weight'].agg('median').reset_index() \
        .merge(edges, on=[fr, to]).rename(columns={'weight' + '_x': 'weight'}) \
        .drop_duplicates([fr, to]) \
        .drop('weight' + '_y', axis=1)

    edge_attr = ['node_id_from', 'node_id_to', 'weight', 'net_type', 'route_type',
                 'sequence', 'unique_agency_id', 'unique_route_id',
                 'unique_trip_id']
    exp_edge_attr = edge_attr
    exp_node_attr = list(nodes.columns)

    nodes['nx_node'] = nodes.apply(parse_nx_node, attr=exp_node_attr, idcol=nodeid, axis=1)
    edges['nx_edge'] = edges.apply(parse_nx_edge, attr_cols=exp_edge_attr, fr=fr, to=to, axis=1)

    graph.add_nodes_from(nodes['nx_node'].to_list())
    graph.add_edges_from(edges['nx_edge'].to_list())

    graph.graph['crs'] = {'init': 'epsg:4326'}
    graph.graph['name'] = edges['unique_agency_id'].unique()[0]

    return graph


def add_transfer_edges(G, headways):
    """ Adds edges between all nodes in the network by taking the avg walking speed/euclidean distance
    between each node. This is done to enable inter-layer edges on transit networks.

    Args:
        G : networkx graph
    """

    def create_edge(orig, dest):
        orig = nodes.iloc[orig]
        dest = nodes.iloc[dest]

        # Do not add loop edges.
        if orig['node_id'] == dest['node_id']:
            return None

        dist = haversine((orig['x'], orig['y']), (dest['x'], dest['y']))

        # Only add a new edge if the distance between the nodes is max 1 kilometer
        if dist > 1:
            return None

        travel_time = dist / MetricTravelSpeeds.WALKING.value * 60
        # 0 means no connection, we prefer a very small number instead.
        # This is to avoid re-adding an edge after the previous add had weight=0 (check loop below)
        if travel_time == 0:
            travel_time = 0.0001

        # Add headway time if available
        if not np.isnan(dest['mean_hw']):
            travel_time += dest['mean_hw']

        return (
            orig['node_id'],
            dest['node_id'],
            {
                'node_id_from': orig['node_id'],
                'node_id_to': dest['node_id'],
                'length': dist,
                'weight': travel_time,
                'travel_time': travel_time,
                'net_type': 'walking_estimation'
            }
        )

    nodes = ox.graph_to_gdfs(G, edges=False)
    nodes = pd.merge(nodes, headways, how='left', left_on='node_id', right_on='unique_stop_id')

    adj_mx = nx.to_numpy_array(G)

    start_time = time.time()
    edges_to_add = []
    # for i in G.nodes():
    for i, node_i in enumerate(G.nodes()):
        for j, node_j in enumerate(G.nodes()):
            if adj_mx[i][j] == 0:
                new_edge = create_edge(i, j)
                if new_edge:
                    edges_to_add.append(new_edge)
                    adj_mx[i][j] = new_edge[2]['weight']
            if adj_mx[j][i] == 0:
                new_edge = create_edge(j, i)
                if new_edge:
                    edges_to_add.append(new_edge)
                    adj_mx[j][i] = new_edge[2]['weight']

            print(f'\r Completed: {len(edges_to_add)} paths.', end='')

    G.add_edges_from(edges_to_add)
    print(f'Total time to connect the network: {round((time.time() - start_time) / 60, 2)} minutes')
    return G
