import io
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pandas as pd

import logging

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def _create_gtfs_calendar(df: pd.DataFrame) -> pd.DataFrame:
    days_of_week = np.array(['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'])
    calendar = pd.DataFrame(columns=['service_id', *days_of_week, 'start_date', 'end_date'])

    # Convert weird date format to proper datetime object
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

    df['day_of_week'] = df['date'].dt.day_name()

    # Still something I should take into consideration but not now
    if len(df[df['exception_type'] == 2]) != 0:
        logger.warning(f"Encountered exceptions in the application of dates\n{df[df['exception_type'] == 2]}")

    service_ids = df['service_id'].unique()

    for service_id in service_ids:
        service_schedule = df[df['service_id'] == service_id]
        days = service_schedule['day_of_week'].values
        days = np.array(list(map(str.lower, days)))
        covered_days = np.isin(days_of_week, days).astype(np.int16)

        start_date = service_schedule['date'].min()
        end_date = service_schedule['date'].max()

        entry = {
            'service_id': service_id,
            'monday': covered_days[0],
            'tuesday': covered_days[1],
            'wednesday': covered_days[2],
            'thursday': covered_days[3],
            'friday': covered_days[4],
            'saturday': covered_days[5],
            'sunday': covered_days[6],
            'start_date': start_date,
            'end_date': end_date
        }

        calendar = calendar.append(entry, ignore_index=True)

    calendar['start_date'] = [e.date().isoformat().replace('-', '') for e in calendar['start_date']]
    calendar['end_date'] = [e.date().isoformat().replace('-', '') for e in calendar['end_date']]

    return calendar


def _generate_gtfs_calendar_txt(path: Path) -> None:
    with ZipFile(path, 'a') as gtfs:
        with gtfs.open('calendar_dates.txt', 'r') as calendar_dates:
            df = pd.read_csv(calendar_dates)
            calendar = _create_gtfs_calendar(df)
        s_buf = io.BytesIO()
        calendar.to_csv(s_buf, index=False)
        # Need to set buffer seek to 0
        s_buf.seek(0)
        gtfs.writestr('calendar.txt', data=s_buf.read())


if __name__=="__main__":
    pass