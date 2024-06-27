import os
from pathlib import Path
import logging

# Logger and output config
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#####################
#### GRAPH GENERATION
#####################
GG_DELETE_EXISTING = os.environ['GG_DELETE_EXISTING']
# TODO Figure out why a mutli-worker setup doesn't work here
GG_NUM_WORKERS = int(os.getenv('GG_NUM_WORKERS', 1))
GG_GTFS_DATA_DIR = Path(os.environ['GG_GTFS_DATA_DIR'])
GG_TRANSIT_GRAPH_DATA_DIR = Path(os.environ['GG_TRANSIT_GRAPH_DATA_DIR'])
GG_CITY_NAME = os.environ['GG_CITY_NAME']

#####################
#### GRAPH GENERATION
#####################

