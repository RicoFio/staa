import datetime

import numpy as np
import pandas as pd
import urbanaccess as ua


def compute_stop_frequencies(ua_feed: ua.feeds) -> pd.DataFrame:
    # Drop all runs where the arrival time is after midnight
    rect_arrivals = ua_feed.stop_times['arrival_time'].apply(lambda x: int(x.split(':')[0]))
    rect_departures = ua_feed.stop_times['departure_time'].apply(lambda x: int(x.split(':')[0]))
    ua_feed.stop_times = ua_feed.stop_times[rect_arrivals < 24]
    ua_feed.stop_times = ua_feed.stop_times[rect_departures < 24]

    ua_feed.stops = ua_feed.stops[ua_feed.stops['unique_agency_id'] != 'nan']
    ua_feed.stops["stop_id"] = ua_feed.stops[["stop_id", "unique_agency_id"]].agg('_'.join, axis=1)
    ua_feed.stop_times = ua_feed.stop_times[ua_feed.stop_times['unique_agency_id'] != 'nan']
    ua_feed.stop_times["stop_id"] = ua_feed.stop_times[["stop_id", "unique_agency_id"]].agg('_'.join, axis=1)

    date = datetime.datetime.strptime(str(ua_feed.calendar_dates.date.unique()[0]), '%Y%m%d')
    day_times = pd.to_datetime(pd.Series([date + datetime.timedelta(hours=e) for e in range(25)]))

    ua_feed.stop_times['arrival_time'] = pd.to_datetime(
        ua_feed.stop_times['arrival_time'].apply(lambda x: str(date.date()) + ' ' + x))
    ua_feed.stop_times['departure_time'] = pd.to_datetime(
        ua_feed.stop_times['departure_time'].apply(lambda x: str(date.date()) + ' ' + x))

    # ## Stop frequencies
    stop_freq = ua_feed.stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]]
    stop_freq = stop_freq.drop_duplicates()

    for h in range(24):
        stop_freq[f"freq_h_{h}"] = np.zeros(len(stop_freq))

    for i in range(len(day_times[:24])):
        served_stops = ua_feed.stop_times[(ua_feed.stop_times.arrival_time >= day_times[i]) & (
                ua_feed.stop_times.arrival_time <= day_times[i + 1])]
        served_stops_count = served_stops.groupby('stop_id').size()
        served_stops_count_ids = served_stops_count.index
        stop_freq.loc[stop_freq['stop_id'].isin(served_stops_count_ids), f'freq_h_{i}'] = served_stops_count.values

    stop_freq.index = stop_freq['stop_id']
    stop_freq = stop_freq.drop(columns=['stop_id'])
    return stop_freq


def compute_segment_frequencies(ua_feed: ua.feeds) -> pd.DataFrame:

    date = datetime.datetime.strptime(str(ua_feed.calendar_dates.date.unique()[0]), '%Y%m%d')
    day_times = pd.to_datetime(pd.Series([date + datetime.timedelta(hours=e) for e in range(25)]))

    # Generate arrival_stop_id for each trip
    ua_feed.stop_times["stop_id_provenance"] = ua_feed.stop_times.groupby('trip_id')["stop_id"].shift(1)

    seg_freq = ua_feed.stop_times[["stop_id", "stop_id_provenance"]]
    seg_freq = seg_freq.dropna()
    seg_freq = seg_freq.drop_duplicates()
    seg_freq.set_index(["stop_id", "stop_id_provenance"], inplace=True)

    for h in range(24):
        seg_freq[f"freq_h_{h}"] = np.zeros(len(seg_freq))

    for i in range(len(day_times[:24])):
        served_stops = ua_feed.stop_times[(ua_feed.stop_times.arrival_time >= day_times[i]) & (
                ua_feed.stop_times.arrival_time <= day_times[i + 1])]
        serv_counts = served_stops.groupby(["stop_id", "stop_id_provenance"]).size()
        seg_freq.loc[serv_counts.index, f"freq_h_{i}"] = serv_counts.values

    return seg_freq
