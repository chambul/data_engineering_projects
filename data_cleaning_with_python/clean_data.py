import pandas as pd

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv'
df = pd.read_csv(url, sep='\t')


pd.set_option('display.max_columns', None) # show all coloumns
pd.set_option('display.max_rows', None)

# observe data
# print(df.head(10))

# check data types of the columns
print("checking initial data types of the columns")
for col, dtype in zip(df.columns, df.dtypes):
    #print(col," : ", dtype) primitive way of doing, so use the below string formatting
    print(f"{col}:{dtype}")

# change data types of the columns
df['item_price'] = df['item_price'].str.replace('$','') #remove $, object data type column
df['item_price'] = df['item_price'].astype('float64')
# print(df['item_price'].head(4))

# Missing values / Null values
df_null_sum = df.isnull().sum()
print('\nnull value counts for each column:')
print(df_null_sum)

print('\nnull value counts as a percentage:')
df_null_mean = df.isnull().mean() * 100
print(df_null_mean)

print('\n items that contain null values:')
d_entries = df.loc[df['choice_description'].isnull(), 'item_name'].unique()
# d_entries = df.loc[df['choice_description'].isnull(), 'item_name'].nunique() # count unique items
print(d_entries)

print('\n replacing null values with regular order')
df['choice_description'] = df['choice_description'].fillna('Regular Order')

reg_order_rows = df[df['choice_description'] == 'Regular Order'].to_string(index=False) # remove indexes
print(reg_order_rows)

print("confirming no null values after replacement: ")
print(df.isnull().sum())

print('number of duplicated rows: ', df.duplicated().sum())

duplicates = df[df.duplicated(keep=False)]
duplicates_sorted = duplicates.sort_values(by='order_id')
print(duplicates_sorted.to_string(index=False))

df.drop_duplicates(inplace=True)
print(' confirming no duplicated rows? : ', df.duplicated().sum())

#3 remove extra spaces
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].str.strip()



df.to_csv('cleaned_data',index=False)