import pandas as pd


def load_kwh_data(file_path):
    """
    Load a processed kwh CSV for reset checking.

    Parameters:
        file_path (str): Path to a harvest_kwh or harvest_interval_kwh CSV.

    Returns:
        dataframe: Loaded dataframe with datetime parsed, sorted by meter and time.
    """
    df = pd.read_csv(file_path, encoding='utf-8')
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)
    return df


def find_meter_resets(df, relative_drop_threshold=0.5):
    """
    Find likely meter reset events: cases where a meter's cumulative kwh reading drops
    sharply (e.g. rolls back to near zero) instead of continuing to climb, and then keeps
    climbing again from the new, lower value.

    Parameters:
        df (dataframe): Meter data with 'datetime', 'meter_name', 'meter_reading' columns.
        relative_drop_threshold (float): Fraction the reading must drop by (relative to the
            previous reading) to be flagged as a reset, e.g. 0.5 = drop of at least 50%.

    Returns:
        dataframe: One row per detected reset, with the meter name, the datetime the reset
        was first seen, the reading right before and after the drop, and the size/percent
        of the drop. Empty dataframe if no resets are found.
    """
    df = df.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)

    events = []

    for meter_name, meter_group in df.groupby('meter_name', sort=False):
        meter_group = meter_group.sort_values('datetime').reset_index(drop=True)
        readings = meter_group['meter_reading'].to_numpy(dtype=float)
        times = meter_group['datetime'].to_numpy()

        if len(readings) < 2:
            continue

        diffs = readings[1:] - readings[:-1]
        previous = readings[:-1]

        # avoid divide by zero when the previous reading is already zero
        safe_previous = previous.copy()
        safe_previous[safe_previous == 0] = 1

        drop_fraction = -diffs / safe_previous
        reset_positions = (diffs < 0) & (drop_fraction >= relative_drop_threshold)

        for i in reset_positions.nonzero()[0]:
            events.append({
                'meter_name': meter_name,
                'reset_datetime': pd.Timestamp(times[i + 1]),
                'reading_before': readings[i],
                'reading_after': readings[i + 1],
                'drop_amount': readings[i] - readings[i + 1],
                'drop_percent': round(drop_fraction[i] * 100, 1),
            })

    events_df = pd.DataFrame(events)

    if not events_df.empty:
        events_df = events_df.sort_values(by=['meter_name', 'reset_datetime']).reset_index(drop=True)

    return events_df
