import pandas as pd
import numpy as np



def load_kwh(data_path):
    """
    Load the preprocessed kwh csv and fix any malformed rows before later steps.

    Parameters:
        data_path (str): Path to the CSV file containing meter original data.

    Returns:
        dataframe: Loaded dataframe with datetime and numeric columns cleaned.
    """
    # load raw data from csv
    df = pd.read_csv(data_path, encoding='utf-8')

    # rename column kwh to meter_reading
    if 'kwh' in df.columns and 'meter_reading' not in df.columns:
        df.rename(columns={'kwh': 'meter_reading'}, inplace=True)

    # convert datetime column to a datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # # drop malformed rows that still could not be fixed
    # bad_input_mask = (
    #     df['datetime'].isna()
    #     | df['meter_name'].isna()
    #     | (df['meter_name'].astype(str).str.strip() == '')
    #     | df['meter_reading'].isna()
    # )

    # if bad_input_mask.any():
    #     print(f"Dropped {bad_input_mask.sum()} malformed input rows before spike cleanup.")
    #     df = df.loc[~bad_input_mask].copy()

    return df


def _typical_positive_step(values):
    """
    Get a typical positive increase size for the cumulative meter readings.
    """
    differences = pd.Series(values).diff()
    positive_differences = differences[differences > 0]

    if positive_differences.empty:
        return 1.0

    upper_limit = positive_differences.quantile(0.90)
    trimmed_positive_differences = positive_differences[positive_differences <= upper_limit]

    if trimmed_positive_differences.empty:
        trimmed_positive_differences = positive_differences

    typical_step = float(trimmed_positive_differences.median())

    if typical_step <= 0:
        typical_step = 1.0

    return typical_step


def remove_invalid_power_rows(meter_group, tiny_power_threshold=1e-20):
    """
    Remove rows where 3_phase_watt_total is a tiny nonzero corrupted value such
    as 5.94e-39. Keep real zeros.
    """
    if '3_phase_watt_total' not in meter_group.columns:
        return meter_group

    power_values = pd.to_numeric(meter_group['3_phase_watt_total'], errors='coerce')
    bad_power_mask = power_values.abs().gt(0) & power_values.abs().lt(tiny_power_threshold)

    if not bad_power_mask.any():
        return meter_group

    return meter_group.loc[~bad_power_mask].reset_index(drop=True)


def remove_kwh_spikes(meter_group, lookback_rows=90, lookback_minutes=60):
    """
    Remove short-lived upward spike blocks when the meter jumps way up and then
    returns back down near its earlier level.
    """
    meter_group = meter_group.sort_values('datetime').reset_index(drop=True).copy()

    if meter_group.shape[0] < 3:
        return meter_group

    values = pd.to_numeric(meter_group['meter_reading'], errors='coerce').to_numpy(dtype=float)
    times = pd.to_datetime(meter_group['datetime']).to_numpy()
    differences = np.diff(values)

    # if the cumulative series never drops, there is no spike block to remove
    if not np.any(differences < 0):
        return meter_group

    typical_step = _typical_positive_step(values)
    return_slack = max(typical_step * 3.0, 1.0)
    spike_height = max(typical_step * 20.0, 10.0)

    suspicious_drop_indexes = np.flatnonzero(differences < -spike_height)

    if suspicious_drop_indexes.size == 0:
        return meter_group

    keep_mask = np.ones(len(meter_group), dtype=bool)

    for drop_index in suspicious_drop_indexes:
        if not keep_mask[drop_index]:
            continue

        return_value = values[drop_index + 1]
        scan_index = drop_index
        rows_scanned = 0
        earliest_allowed_time = pd.Timestamp(times[drop_index + 1]) - pd.Timedelta(minutes=lookback_minutes)

        while (
            scan_index >= 0
            and rows_scanned < lookback_rows
            and pd.Timestamp(times[scan_index]) >= earliest_allowed_time
            and values[scan_index] > return_value + spike_height
        ):
            scan_index -= 1
            rows_scanned += 1

        block_start_index = scan_index + 1

        if block_start_index == 0:
            continue

        previous_good_value = values[block_start_index - 1]
        allowed_return_upper = previous_good_value + (return_slack * (drop_index - block_start_index + 3))
        allowed_return_lower = previous_good_value - return_slack

        returned_to_baseline = allowed_return_lower <= return_value <= allowed_return_upper

        if returned_to_baseline:
            keep_mask[block_start_index:drop_index + 1] = False

    return meter_group.loc[keep_mask].reset_index(drop=True)


def clean_kwh_spikes(df):
    """
    Remove corrupted power rows and spike rows from the loaded kwh dataframe.

    Parameters:
        df (dataframe): Loaded kwh dataframe.

    Returns:
        dataframe: Cleaned dataframe with spike rows removed.
    """
    all_meter_groups = []

    for meter_name, meter_group in df.groupby('meter_name', sort=False):
        meter_group = meter_group.sort_values('datetime').reset_index(drop=True)

        meter_group = remove_invalid_power_rows(meter_group)
        meter_group = remove_kwh_spikes(meter_group)

        if not meter_group.empty:
            all_meter_groups.append(meter_group)

    if len(all_meter_groups) == 0:
        return df.iloc[0:0].copy()

    result = pd.concat(all_meter_groups, ignore_index=True)
    result = result.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)
    return result


def process_kwh(df):
    """
    Interpolate meter readings to exact 15 minute intervals. Contains boolean 'interpolated'
    column to indicate if the row was interpolated or not, and boolean 'is_exact' column
    to indicate if row is at an exact 15 minute interval.

    Parameters:
        df (dataframe): Cleaned dataframe containing meter original data.
    
    Returns:
        dataframe: Dataframe with interpolated kwh readings at exact 15 minute intervals.
    """
    df = df.copy()

    # drop the 3_phase_watt_total column as its not needed for kwh interpolation
    if '3_phase_watt_total' in df.columns:
        df.drop('3_phase_watt_total', axis=1, inplace=True)

    # make sure datetime is datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    df['is_exact'] = False
    df['interpolated'] = False
    
    all_rows = []

    for meter_name, meter_group in df.groupby('meter_name', sort=False):
        meter_group = meter_group.sort_values('datetime').reset_index(drop=True)

        meter_times = pd.DatetimeIndex(meter_group['datetime'])
        meter_readings = meter_group['meter_reading'].to_numpy()
        meter_length = len(meter_group)
        
        # create target intervals
        start = meter_group['datetime'].min().floor('15min')
        end = meter_group['datetime'].max().ceil('15min')
        
        # create list of every exact 15min timestamp that SHOULD exist for that meter
        target_intervals = pd.date_range(start=start, end=end, freq='15min')
        
        # for each target interval check if a real reading exists
        for interval in target_intervals:
            interval_index = meter_times.searchsorted(interval)
            exact_match_exists = (
                interval_index < meter_length and meter_times[interval_index] == interval
            )

            if exact_match_exists:
                # real reading exists at this exact interval
                row = meter_group.iloc[interval_index].copy()
                row['is_exact'] = True
                row['interpolated'] = False
                all_rows.append(row)
            else:
                # only look within 15 minutes either side
                window = pd.Timedelta(minutes=15)

                before_index = interval_index - 1
                after_index = interval_index

                valid_before = before_index >= 0
                valid_after = after_index < meter_length

                # only interpolate if we have readings on BOTH sides
                if valid_before and valid_after:
                    time_before = meter_times[before_index]
                    time_after = meter_times[after_index]

                    close_enough_before = (interval - time_before) <= window
                    close_enough_after = (time_after - interval) <= window

                    if close_enough_before and close_enough_after:
                        reading_before = meter_readings[before_index]
                        reading_after = meter_readings[after_index]

                        time_diff = (time_after - time_before).total_seconds()
                        reading_diff = reading_after - reading_before

                        if time_diff == 0 or reading_diff == 0:
                            # if no time difference or no reading difference, use the before reading
                            estimated_kwh = reading_before
                        else:
                            slope = round(reading_diff / time_diff, 4)
                            sec_before_interval = (interval - time_before).total_seconds()
                            estimated_kwh = reading_before + (slope * sec_before_interval)

                        # create interpolated row
                        new_row = meter_group.iloc[before_index].copy()
                        new_row['datetime'] = interval
                        new_row['meter_reading'] = estimated_kwh
                        new_row['is_exact'] = True
                        new_row['interpolated'] = True

                        # add new interpolated row to list
                        all_rows.append(new_row)
                    
                    else:
                        # no close enough readings on one or both sides, skip this interval
                        pass
                else:
                    # no close enough readings on one or both sides, skip this interval
                    pass

        # keep original nonexact interval rows:

        # creates T/F column for whether the datetime is in exact interval, then filter to only nonexact rows with ~
        non_exact = meter_group[~meter_group['datetime'].isin(target_intervals)]

        # add non exact rows to list of all rows
        for _, row in non_exact.iterrows():     # _ is a placeholder for row index (don't need it)
            all_rows.append(row)

    # create final dataframe from list of all rows, sort by meter name and datetime, reset index
    result = pd.DataFrame(all_rows)
    result = result.sort_values(by=['meter_name', 'datetime']).reset_index(drop=True)
    
    return result


def interval_kwh(df):
    """
    Get only the rows from the dataframe that are at exact 15 minute intervals, and drop the
    'is_exact' and 'interpolated' columns.

    Parameters:
        df (dataframe): The dataframe containing the meter readings and interpolated readings
        at exact 15 minute intervals.
    
    Returns:
        dataframe: Dataframe with only the rows at exact 15 minute intervals and removed columns.
    """
    interval_df = df[df['is_exact'] == True].copy()
    interval_df.drop(['is_exact', 'interpolated'], axis=1, inplace=True)
    return interval_df


def duplicate_check(df):
    """
    Check for duplicate rows in the dataframe and print them.

    Parameters:
        df (dataframe): The dataframe to check for duplicates.
    """
    duplicate_data = df[df.duplicated(keep=False)]
    if duplicate_data.empty:
        print("No duplicate rows found.")
    else:
        print("Duplicate rows found:")
        print(duplicate_data)


def meter_list(csv):
    """
    Print the list of unique meter names in the CSV file.
    
    Parameters:
        csv (str): Path to the CSV file.
    """
    df = pd.read_csv(csv, encoding='utf-8')
    print('List of meter names: \n', df['meter_name'].unique())
