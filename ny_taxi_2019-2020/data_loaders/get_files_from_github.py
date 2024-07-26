import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def load_data(*args, **kwargs):
    
    # Specify your data loading logic here
    
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    csv_file = 'green_taxi_data.csv'
    files_to_merge = []
    year = 1
    for month in range(1, 13):
        if year == 1:
            if month < 10:
                url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-0{month}.csv.gz'
            else:
                url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-{month}.csv.gz'
        elif year == 2:
            if month < 10:
                url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-0{month}.csv.gz'
            else:
                url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'
        else:
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2021-0{month}.csv.gz'

        df1 = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        
        if month < 2:
            df1.to_csv(csv_file)
        else:
            df2 = pd.read_csv(csv_file)
            files_to_merge.append(df1)
            files_to_merge.append(df2)
            merged_df = pd.concat(files_to_merge)
            merged_df.to_csv(csv_file)
        files_to_merge = []

        if month == 12:
            month = 1
            year += 1

        if year == 3 and month == 7:
            break
    
    return pd.read_csv(csv_file)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
