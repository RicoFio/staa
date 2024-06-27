import os
from pathlib import Path
from typing import Optional, List, Tuple, Union

import pandas as pd
import requests
from multiprocessing.pool import ThreadPool
import datetime
import logging
import subprocess

from zipfile import ZipFile

from transit_feed_providers import TransitFeedProviders
from exceptions import GTFSDownloadException

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

DATA_PATH = os.environ.get("DATA_PATH", './data/full_gtfs_files')
AGENCIES = ['GVB', 'IFF:GVB', 'IFF:NS', 'IFF:NSI', 'IFF:RNET']

ON_LISA = bool(os.environ.get("ON_LISA", False))


def _create_gtfs_urls(provider: TransitFeedProviders,
                      start_date: datetime.date,
                      end_date: Optional[datetime.date] = None) -> List[Tuple[Path, str]]:
    """

    :param provider:
    :param start_date:
    :param end_date:
    :return:
    """
    if not end_date:
        end_date = datetime.date.today()

    if not start_date < end_date:
        raise Exception("start_date cannot be before or on end_date; it has to be further back in time")

    def perdelta(start: datetime.date, end: datetime.date, delta: datetime.timedelta = datetime.timedelta(days=1)):
        curr = start
        while curr < end:
            yield curr
            curr += delta

    day_dates = perdelta(start_date, end_date, )

    return list(map(lambda x: (Path(f"{DATA_PATH}/ov-gtfs-{x.isoformat().replace('-', '')}.zip"),
                               f"https://transitfeeds.com/p/{provider.value}/{x.isoformat().replace('-', '')}/download"),
                    day_dates)
                )


def _fetch_gtfs(path_and_uri: Tuple[Path, str]) -> Union[Path, GTFSDownloadException]:
    path, uri = path_and_uri

    # Download GTFS if it does not exist
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            logger.info(f"downloading {uri}")
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
        else:
            return GTFSDownloadException(f"unable to download {uri}")
    logger.info(f"stored {uri} in {path}")
    return path


def _filter_gtfs(path: Union[Path, GTFSDownloadException]) -> Union[Path, GTFSDownloadException]:
    if isinstance(path, GTFSDownloadException):
        return path
    else:
        in_path_file_ext = path.suffix
        in_path = path.with_suffix('')
        out_path = in_path.with_name(f"{in_path.name}-filtered-by-{'_'.join(AGENCIES)}").with_suffix(in_path_file_ext)

        # Needed to make sure that the agencies that we want to filter for
        # are actually available
        with ZipFile(path, 'r') as gtfs:
            with gtfs.open('agency.txt') as agencies:
                df = pd.read_csv(agencies)
                available_agencies = set(df.agency_id.to_list())

        if not os.path.exists(out_path):
            available_agencies = available_agencies.intersection(set(AGENCIES))
            args = [e for sublist in [["-extract-agency", agency] for agency in available_agencies] for e in sublist]
            tool = "transitland" if not ON_LISA else "/home/fiorista/thesis/bin/transitland"
            subprocess.run([tool, "extract", *args, path, out_path])

        # Remove big zip
        # os.remove(path)

        return out_path


def download_and_store_gtfs(start_date: datetime.date, end_date: datetime.date, provider: TransitFeedProviders):
    urls = _create_gtfs_urls(provider, start_date, end_date)

    downloaded_paths = []
    total_space = 0

    results = ThreadPool(8).imap_unordered(_fetch_gtfs, urls)
    filter_results = ThreadPool(6).imap_unordered(_filter_gtfs, results)
    for path_or_exception in filter_results:
        if not isinstance(path_or_exception, GTFSDownloadException):
            path = path_or_exception
            logger.info(f"downloaded and processed {path}")
            downloaded_paths.append(path)
            total_space += path.stat().st_size
        else:
            logger.warning(str(path_or_exception))

    logger.info(f"###\n"
                f"Processed {len(downloaded_paths)} feeds\n"
                f"Amounting to {total_space / (1024.0 * 1024.0)} MB\n"
                f"###")


if __name__ == "__main__":
    # Set start and end date for feeds
    start_date = datetime.date(2019, 1, 1)
    end_date = datetime.date(2021, 12, 31)

    # Start job
    download_and_store_gtfs(start_date, end_date, provider=TransitFeedProviders.OV)
