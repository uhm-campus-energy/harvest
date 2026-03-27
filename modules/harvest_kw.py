import pandas as pd

def load_data(data_path, info_path):
    """
    Load meter data and meter info from CSV files. Prepare dataframe by cleaning and formatting.

    Parameters:
        data_path (str): Path to the meter data CSV.
        info_path (str): Path to the meter info CSV (contains meter model info).

    Returns:
        df (dataframe): Cleaned and formatted meter data.
        info_df (dataframe): Cleaned meter info data.
    """
    # load data from csv files into dataframes
    df = pd.read_csv(data_path, encoding='utf-8')
    info_df = pd.read_csv(info_path, encoding='utf-8')

    # remove total watt hour column, it is not relevant to these calculations
    if 'total_watt_hour' in df.columns:
        df.drop('total_watt_hour', axis=1, inplace=True)

    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values(by=['meter_name', 'datetime'])

    # remove extra columns
    info_df.drop(columns={'header1', 'header2'}, axis=1, inplace=True)

    # ensure all meter names are uniform
    info_df['meter_name'] = info_df['meter_name'].str.replace(' ', '_')

    return df, info_df

def filter_time_frame(df, start, end):
    """
    Filter meter data to only include rows within the specified datetime range.

    Parameters:
        df (dataframe): Meter data.
        start (datetime): Start datetime.
        end (datetime): End datetime.

    Returns:
        filtered_df (dataframe): Filtered meter data within the specified datetime range.
    """
    filtered_df = df[(df['datetime'] >= start) & (df['datetime'] <= end)].copy()
    return filtered_df


def process_kw_data(df, info_df):
    """
    Process meter data to calculate average kw (kw = 3 phase watt total) per 15 minute interval,
    adjusting for meter model. PQM2 meters report in kw, while EPM7000 meters report in watts 
    so will only divide EMP7000 meters by 1000.

    Parameters:
        df (dataframe): Meter data.
        info_df (dataframe): Meter model info.

    Returns:
        result_df (dataframe): Processed data with average kw per 15 minute interval.
    """
    # create set of all meters of EPM7000 model
    model_check = set(info_df[info_df['meter_model'].str.contains('EPM7000')]['meter_name'])

    # create column that contains the interval that the row belongs to
    df['interval'] = df['datetime'].dt.floor('15min')

    # create result dataframe categorized by the meter name and interval calculate 
    # the average of the 3 phase watt total
    result_df = df.groupby(['meter_name', 'interval'])['3_phase_watt_total'].mean().reset_index()

    # create mean_kw column, dividing by 1000 for EPM7000 meters
    result_df['mean_kw'] = result_df['3_phase_watt_total'].copy()
    result_df.loc[result_df['meter_name'].isin(model_check), 'mean_kw'] /= 1000

    # rename and reorder columns, delete 3 phase column
    result_df.rename(columns={'interval': 'datetime'}, inplace=True)
    result_df.drop('3_phase_watt_total', axis=1, inplace=True)
    result_df = result_df[['datetime', 'meter_name', 'mean_kw']]

    return result_df
