import os
from typing import Tuple, Union
from pathlib import Path
import re
from multiprocessing.pool import ThreadPool

import pandas as pd
import gtfs_kit as gk
from exceptions import GTFSDateError

import datetime

import logging

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

ORIGIN_DATA_DIR = Path(os.getenv('ORIGIN_DATA_DIR', './data/filtered_gtfs_files'))
TARGET_DATA_DIR = Path(os.getenv('TARGET_DATA_DIR', './data/day_gtfs_files'))
NUM_WORKERS = os.getenv('NUM_WORKERS', 4)

ON_LISA = bool(os.environ.get("ON_LISA", False))


def _extract_and_store_gtfs_for_dates(entry: Tuple) -> Union[Path, GTFSDateError]:
    # Check if gtfs file needs to be read in
    logger.info(f"reading in file: {entry[1][0]}")
    curr_path = entry[1][0]
    feed = gk.read_feed(curr_path, dist_units='km')

    # Extract current date
    d = entry[0].date()
    curr_date = d.isoformat().replace('-', '')

    # Make sure date is in available dates
    try:
        assert curr_date in feed.get_dates()
    except:
        logger.warning(f"could not find date {curr_date} in {feed.get_dates()}")
        return GTFSDateError("failed to find {curr_date} in {feed.get_dates()}")

    restricted_feed = feed.restrict_to_dates(dates=[curr_date])
    reduced_file_path = TARGET_DATA_DIR.joinpath(f'filtered-ov-gtfs-{curr_date}.zip')
    restricted_feed.write(reduced_file_path)

    return reduced_file_path


def extract_and_store_gtfs_for_dates(dates: pd.DataFrame) -> None:
    total_space = 0
    extracted_days_paths = []

    results = ThreadPool(NUM_WORKERS).imap_unordered(_extract_and_store_gtfs_for_dates, dates.iterrows())
    for path in results:
        if isinstance(path, GTFSDateError):
            continue
        extracted_days_paths.append(path)
        total_space += path.stat().st_size

    n_paths_processed = len(extracted_days_paths)
    extracted_dates = [datetime.datetime.strptime(re.findall(r'\d{8}', str(path))[0], '%Y%m%d') for path in extracted_days_paths]
    logger.info(f"###\n"
                f"Extracted {n_paths_processed} feed{'s'if n_paths_processed > 1 else ''}\n"
                f"Dates extracted ranging from {min(extracted_dates)} to {max(extracted_dates)}\n"
                f"Amounting to {total_space / (1024.0 * 1024.0)} MB\n"
                f"###")


if __name__ == "__main__":
    # Load all GTFS files
    all_gtfs_files = [ORIGIN_DATA_DIR.joinpath(e) for e in os.listdir(ORIGIN_DATA_DIR) if Path(e).suffix == '.zip']
    logger.info(f"Identified {len(all_gtfs_files)} candidate GTFS archives")
    # Get all dates from the GTFS files
    dates = [re.findall(r'\d{8}', str(path))[0] for path in all_gtfs_files]
    logger.info(f"Identified {len(dates)} dates for the GTFS archives")
    series_dates = pd.to_datetime(pd.Series(dates)).dt.date
    df_dates = pd.DataFrame({'GTFS_File': all_gtfs_files}, index=pd.DatetimeIndex(series_dates))
    # Forwardfill the GTFS files for the corresponding dates to get the correct mapping
    # GTFS files are looking up to 30 weeks ahead so this should never cause failure
    # and makes sure that we're using the latest GTFS feed
    df_dates = df_dates.resample('D').ffill()
    df_dates['Day'] = pd.Series(pd.Series(df_dates.index).dt.day_name().values, index=df_dates.index)
    # Extract all mondays and corresponding GTFS files
    mondays = df_dates[df_dates['Day'] == 'Monday']
    # Generate files
    extract_and_store_gtfs_for_dates(mondays)
