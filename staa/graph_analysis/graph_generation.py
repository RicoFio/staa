from .utils.graph_helper_utils import (
    ua_transit_network_to_nx,
    append_length_attribute,
    append_hourly_edge_frequency_attribute,
    append_hourly_stop_frequency_attribute,
)
from .utils.osm_utils import get_bbox
from .utils.frequency_computation_utils import (
    compute_stop_frequencies,
    compute_segment_frequencies,
)

from .exceptions import GraphGenerationError
from ..settings import (
    logger,
    GG_DELETE_EXISTING,
    GG_NUM_WORKERS,
    GG_GTFS_DATA_DIR,
    GG_TRANSIT_GRAPH_DATA_DIR,
    GG_CITY_NAME,
)

import os
import re
import time
from typing import Tuple, List, Union
from pathlib import Path
from zipfile import ZipFile
from multiprocessing.pool import ThreadPool

import networkx as nx
import urbanaccess as ua
# Prevent UA to log unnecessary output
from urbanaccess.config import settings
settings.log_consolse = False


def _remove_files_in_dir(curr_run_dir: Union[Path, str]):
    for f in os.listdir(curr_run_dir):
        os.remove(curr_run_dir.joinpath(f))


def _generate_and_store_graphs(args: Tuple[Tuple[float, float, float, float], Path]) -> Union[
    Path, GraphGenerationError]:
    logger.debug(f"received: {args}")
    bbox, gtfs_file = args

    # Load GTFS
    curr_run_dir = GG_TRANSIT_GRAPH_DATA_DIR.joinpath(gtfs_file.with_suffix('').name)
    if os.path.exists(curr_run_dir):
        logger.warning(f"Directory {curr_run_dir} already exists{' -> removing.' if GG_DELETE_EXISTING else ''}")
        if GG_DELETE_EXISTING:
            for file in os.listdir(curr_run_dir):
                os.remove(curr_run_dir.joinpath(file))
            os.rmdir(curr_run_dir)
        else:
            return curr_run_dir
    else:
        os.mkdir(curr_run_dir)

    with ZipFile(gtfs_file) as ref:
        ref.extractall(curr_run_dir)
        loaded_feeds = ua.gtfs.load.gtfsfeed_to_df(gtfsfeed_path=str(curr_run_dir.absolute()),
                                                   validation=True,
                                                   verbose=True,
                                                   bbox=bbox,
                                                   remove_stops_outsidebbox=True,
                                                   append_definitions=True)
        _remove_files_in_dir(curr_run_dir)

    # Create the transit network graph from GTFS feeds using the urbanaccess library
    logger.debug(loaded_feeds.calendar_dates.columns)
    try:
        transit_net = ua.gtfs.network.create_transit_net(
            gtfsfeeds_dfs=loaded_feeds,
            calendar_dates_lookup={'unique_feed_id': f"{gtfs_file.with_suffix('').name}_1"},
            day='monday',
            timerange=['07:00:00', '09:00:00'],
        )

        # Generate transit graph WITHOUT headways
        G_transit = ua_transit_network_to_nx(transit_net)
        G_transit = append_length_attribute(G_transit)

        # Generate stop frequency dataframe
        stop_freq_df = compute_stop_frequencies(loaded_feeds)
        seg_freq_df = compute_segment_frequencies(loaded_feeds)

        # Append frequencies as attributes to the graph
        append_hourly_stop_frequency_attribute(G_transit, stop_freq_df)
        append_hourly_edge_frequency_attribute(G_transit, seg_freq_df)

        # Extract the date from the current GTFS file
        date = re.findall(r'\d+', str(gtfs_file))[0]

        nx.write_gpickle(G_transit, curr_run_dir.joinpath(f'ams_pt_network_monday_{date}.gpickle'))
        nx.write_gml(G_transit, curr_run_dir.joinpath(f'ams_pt_network_monday_{date}.gml'))
    except Exception as e:
        logger.error(str(e))
        logger.error(f"With columns {loaded_feeds.calendar_dates.columns}\n"
                     f"And values {loaded_feeds.calendar_dates.head(10)}")
        _remove_files_in_dir(curr_run_dir)
        os.rmdir(curr_run_dir)
        return GraphGenerationError(curr_run_dir)

    return curr_run_dir


def generate_transit_graphs(bbox_dict: dict, gtfs_day_files: List[Path]):
    # (lng_max, lat_min, lng_min, lat_max)
    bbox = (
        bbox_dict['west'],
        bbox_dict['south'],
        bbox_dict['east'],
        bbox_dict['north'],
    )

    stored_graphs = []
    not_processed = []
    total_space = 0
    start = time.time()

    inputs = [[bbox, gtfs_day_file] for gtfs_day_file in gtfs_day_files]
    logger.debug(inputs)
    results = ThreadPool(GG_NUM_WORKERS).imap_unordered(_generate_and_store_graphs, inputs)

    for r in results:
        if isinstance(r, GraphGenerationError):
            not_processed.append(str(r))
        stored_graphs.append(r)
        total_space += r.stat().st_size

    logger.info(f"###\n"
                f"Processed {len(stored_graphs)} graphs\n"
                f"Amounting to {total_space / (1024.0 * 1024.0)} MB\n"
                f"Took {time.time() - start} seconds\n"
                f"###\n"
                f"Could not process: {not_processed}\n"
                f"###")


def run_graph_generation():
    # Aggregate needed data
    bbox_dict = get_bbox(GG_CITY_NAME)
    all_gtfs_files = [GG_GTFS_DATA_DIR.joinpath(e) for e in os.listdir(GG_GTFS_DATA_DIR) if Path(e).suffix == '.zip']

    # Run the core part
    generate_transit_graphs(bbox_dict, all_gtfs_files)


if __name__ == '__main__':
    run_graph_generation()
