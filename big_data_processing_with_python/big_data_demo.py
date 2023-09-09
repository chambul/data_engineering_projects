## Working with big data: reading in chunks

import pandas as pd

# modofy chunksize to reduce run time.
# for df in pd.read_csv('pokemon_data.csv', chunksize=5):
#     print("CHUNK DF")
#     print(df)


# create a new empty df, then periodically append the data
df = pd.read_csv('pokemon_data.csv')
main_df = pd.DataFrame(columns=df.columns)

for df in pd.read_csv('pokemon_data.csv', chunksize=5):
    results = df.groupby(['Type 1']).count()
    main_df = pd.concat([main_df,results])

print(main_df)