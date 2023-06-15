import sys
import os
import pickle
import pandas as pd

year = int(sys.argv[1]) # 2022
month = int(sys.argv[2]) # 1


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)



categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

taxi_type = 'yellow'

df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet')


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


# Print the predicted mean value of the trip
print (y_pred.mean())

df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

df_result = pd.DataFrame(
    {'ride_id': df.ride_id,
    'predictions': y_pred})

dir_name = "output"

# Check if the directory already exists
if not os.path.exists(dir_name):
    # Create the directory
    os.mkdir(dir_name)

output_file = f'{dir_name}/predictions_{taxi_type}_{year:04d}-{month:02d}.parquet'

df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)

