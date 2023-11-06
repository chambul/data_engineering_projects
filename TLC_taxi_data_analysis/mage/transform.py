import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):

    df['tpep_pickup_datetime']= pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']= pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    dim_datetime = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)

    # pickup: extract year, month, day, hour, weekday
    dim_datetime['pickup_year'] = df['tpep_pickup_datetime'].dt.year
    dim_datetime['pickup_month'] = df['tpep_pickup_datetime'].dt.month
    dim_datetime['pickup_day'] = df['tpep_pickup_datetime'].dt.day
    dim_datetime['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    dim_datetime['pickup_weekday'] = df['tpep_pickup_datetime'].dt.weekday

    # dropoff: extract year, month, day, hour, weekday
    dim_datetime['dropoff_year'] = df['tpep_dropoff_datetime'].dt.year
    dim_datetime['dropoff_month'] = df['tpep_dropoff_datetime'].dt.month
    dim_datetime['dropoff_day'] = df['tpep_dropoff_datetime'].dt.day
    dim_datetime['dropoff_hour'] = df['tpep_dropoff_datetime'].dt.hour
    dim_datetime['dropoff_weekday'] = df['tpep_dropoff_datetime'].dt.weekday

    #create id using index
    dim_datetime['datetime_id'] = dim_datetime.index

    # reorder columns
    dim_datetime = dim_datetime[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
                                'tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month', 'dropoff_year', 'dropoff_weekday']]


   # dim_pickup_location table
    dim_pickup_location = df[['pickup_latitude','pickup_longitude']].reset_index(drop=True)
    dim_pickup_location['pickup_loc_id'] = dim_pickup_location.index
    dim_pickup_location = dim_pickup_location[['pickup_loc_id','pickup_latitude','pickup_longitude']]

   # dim_dropoff_location table
    dim_dropoff_location = df[['dropoff_latitude', 'dropoff_longitude']].reset_index(drop=True)
    dim_dropoff_location['dropoff_loc_id'] = dim_dropoff_location.index
    dim_dropoff_location = dim_dropoff_location[['dropoff_loc_id', 'dropoff_latitude', 'dropoff_longitude']]

#dim_ratecode table
    ratecode_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    dim_ratecode = df[['RatecodeID']].reset_index(drop=True)
    dim_ratecode['rate_code_id'] = dim_ratecode.index
    dim_ratecode['rate_code_name'] = dim_ratecode['RatecodeID'].map(ratecode_type) # map ratecode name based on id
    dim_ratecode = dim_ratecode[['rate_code_id','RatecodeID','rate_code_name']]

  #dim_payment_type table
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    dim_payment_type = df[['payment_type']].reset_index(drop=True)
    dim_payment_type['payment_type_id'] = dim_payment_type.index
    dim_payment_type['payment_type_name'] = dim_payment_type['payment_type'].map(payment_type_name)
    dim_payment_type = dim_payment_type[['payment_type_id','payment_type','payment_type_name']]

  # merge based on initial indexes
    fact_taxi = df.merge(dim_datetime, left_on='trip_id', right_on='datetime_id') \
              .merge(dim_payment_type, left_on='trip_id', right_on='payment_type_id') \
              .merge(dim_ratecode, left_on='trip_id', right_on='rate_code_id') \
              .merge(dim_pickup_location, left_on='trip_id', right_on='pickup_loc_id') \
              .merge(dim_dropoff_location, left_on='trip_id', right_on='dropoff_loc_id')\
             [['trip_id','VendorID', 'datetime_id', 'passenger_count',
               'trip_distance', 'rate_code_id', 'store_and_fwd_flag', 'pickup_loc_id', 'dropoff_loc_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]

    return {"dim_datetime":dim_datetime.to_dict(orient="dict"),
    "dim_ratecode":dim_ratecode.to_dict(orient="dict"),
    "dim_pickup_location":dim_pickup_location.to_dict(orient="dict"),
    "dim_dropoff_location":dim_dropoff_location.to_dict(orient="dict"),
    "dim_payment_type":dim_payment_type.to_dict(orient="dict"),
    "fact_taxi":fact_taxi.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
