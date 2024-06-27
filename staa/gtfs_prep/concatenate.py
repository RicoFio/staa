import os
import re
import datetime
import logging
from pathlib import Path
from typing import List

import numpy as np
import tqdm
from gtfsmerger import GTFSMerger

DATA_PATH = os.environ.get("DATA_PATH", './data')

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class TreeGTFSMerger:
    def __init__(self, fpaths: List[Path], max_size: int, parent: bool = True) -> None:
        self.fpaths = fpaths
        self.max_size = max_size
        self.parent = parent

    def recursive_merge(self) -> str:
        while len(self.fpaths) > self.max_size:
            splits = np.array_split(self.fpaths, self.max_size)
            merged_paths = []

            generator = tqdm.tqdm(splits) if self.parent else splits

            for split in generator:
                merged_paths.append(TreeGTFSMerger(split, self.max_size, parent=False).recursive_merge())

            self.fpaths = merged_paths

        out_path = None

        if len(self.fpaths) > 1:
            dates_str = [re.findall(r'\d+', str(f)) for f in self.fpaths]
            dates_str = [item for sublist in dates_str for item in sublist]
            date_converter = lambda x: x[0:4] + '-' + x[4:6] + '-' + x[6:9]
            dates = [datetime.date.fromisoformat(date_converter(d)) for d in dates_str]

            # Generate out path
            start_date = min(dates).isoformat().replace('-', '')
            end_date = max(dates).isoformat().replace('-', '')
            base_path = os.path.dirname(DATA_PATH)
            out_path = os.path.join(base_path, f"merged-gtfs-{start_date}-{end_date}.zip")

            if not os.path.exists(out_path):
                # Merge gtfs
                gtfs_merger = GTFSMerger()
                gtfs_merger.merge_from_fpaths(self.fpaths)
                # Store zip
                gtfs_merger.get_zipfile(out_path)
                del gtfs_merger

        elif len(self.fpaths) == 1:
            out_path = self.fpaths[0]

        return out_path


if __name__ == "__main__":
    logger.info(f"currently in path {DATA_PATH}")
    fpaths = [Path(DATA_PATH).absolute().joinpath(f) for f in os.listdir(DATA_PATH)] #if 'filtered' in f]
    logger.info(f"considering files: {fpaths}")
    tm = TreeGTFSMerger(fpaths, max_size=2)
    resulting_path = tm.recursive_merge()
    logger.info(f"Successfully merged all filtered GTFS and stored in {resulting_path}")
