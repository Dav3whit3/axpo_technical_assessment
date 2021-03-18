import pandas as pd
import numpy as np
import time

speed_df = pd.read_csv("DataOps_test/dataset_speed_optimization.csv")

print()
print("Running time optimization:\n")
print("""
      The following runtimes (in seconds) represent 3 different ways to apply the same formula.
      CSV used: dataset_speed_optimization.csv\n
""")
print()
########################################################################################

def function_to_apply(lat, lon):
    a = np.sin(lat / 2) ** 2 + np.cos(lon) * np.sin(lon / 2) ** 2

    return a


df = speed_df.copy()

list_results = []
start = time.time()
for i in range(0, len(df)):
    r = function_to_apply(df.iloc[i]['latitude'], df.iloc[i]['longitude'])
    list_results.append(r)

df['distance'] = list_results
elapsed_time_fl = (time.time() - start)
print(f"Time taken using iloc: {elapsed_time_fl}")
########################################################################################


def ftapply(row):
    return np.sin(row['latitude']/2)**2 + np.cos(row['longitude']) * np.sin(row['longitude']/2)**2


df = speed_df.copy()

start = time.time()
df['distance'] = df.apply(ftapply, axis=1)
elapsed_time_fl = (time.time() - start)
print(f"Time taken using pandas apply: {elapsed_time_fl}")

########################################################################################

df = speed_df.copy()

start = time.time()

df['distance'] = np.sin(df['latitude']/2)**2 + np.cos(df['longitude']) * np.sin(df['longitude']/2)**2
elapsed_time_fl = (time.time() - start)

print(f"Time taken when creating a new column vectorizing the operation: {elapsed_time_fl}\n")

########################################################################################

print(f"Memory usage optimization:\n")
print("""
    The following table will show the percentual change of memory usage (in bytes) after optimizing its data types.
    CSV used: dataset_memory_optimization.csv\n
""")

df = pd.read_csv("DataOps_test/dataset_memory_optimization.csv")
initial_memory_usage = df.memory_usage(deep=True)

print("Casting object-type columns to 'category'")
type_object = df.select_dtypes(include='object').columns.to_list()
for col in type_object:
    df[f'{col}'] = df[f'{col}'].astype("category")

print("Downscaling numeric-type columns\n")

df["ean_hotel_id"] = pd.to_numeric(df["ean_hotel_id"], downcast="unsigned")
float_type = df.select_dtypes(include='float').columns.to_list()
df[float_type] = df[float_type].apply(pd.to_numeric, downcast="float")

new_memory_usage = pd.DataFrame(df.memory_usage(deep=True))

result = pd.concat([initial_memory_usage, new_memory_usage], axis=1)
result.columns = ["initial_memory_usage", "new_memory_usage"]
result['percentual_change'] = (result['new_memory_usage'] - result['initial_memory_usage']) / result['initial_memory_usage']*100
result['percentual_change'] = result['percentual_change'].round(decimals=2).astype(str) + '%'

print(result)
