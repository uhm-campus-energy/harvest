import pandas as pd

# finding missing kw data 

def load_kw_data(file_path):
    """
    Load processed kw data from a CSV file.

    Parameters:
        file_path (str): Path to the CSV file.

    Returns: 
        df (dataframe): Loaded kw data.
    """
    df = pd.read_csv(file_path, encoding='utf-8')
    df['datetime'] = pd.to_datetime(df['datetime'])
    #df.drop(columns=['mean_kw'], inplace=True)

    return df

def find_missing_kw_data(file_path, start_month, end_month):
    """
    Find missing kw data in the specified date range.
    Parameters:
        file_path (str): Path to the CSV file containing kw data.
        start_month (int): Start month (1-12).
        end_month (int): End month (1-12).

    Returns:
        summary_df (dataframe): Summary dataframe showing percentage of data present for each meter and month
    """
    import calendar

    df = load_kw_data(file_path)

    # filter data to only include months within date range
    df = df[df['datetime'].dt.month.isin(range(start_month, end_month + 1))]

    # create year and month columns
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month

    df.drop(columns=['datetime'], inplace=True)

    results = []

    for (meter_name, year, month), group in df.groupby(['meter_name', 'year', 'month']):
        # calculate expected number of readings
        intervals_in_day = 96  # 15-minute intervals in a day
        days_in_month = calendar.monthrange(year, month)[1]
        expected_count = days_in_month * intervals_in_day

        # count actual readings (non-null kw values)
        actual_count = group['mean_kw'].notna().sum()

        # percentage of data present
        data_perc = actual_count / expected_count * 100

        #missing_count = expected_count - actual_count

        month_year = f'{calendar.month_abbr[month]}\'{year %100}'

        results.append({
            'meter_name': meter_name,
            'month_year': month_year,
            'data_perc': round(data_perc, 1)
        })

        results_df = pd.DataFrame(results)

        # pivot the results for easier readability
        summary_df = results_df.pivot(index='meter_name', columns='month_year', values='data_perc')

        #auto reorder columns by month and year

    return summary_df