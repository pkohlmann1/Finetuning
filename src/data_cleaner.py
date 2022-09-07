import pandas as pd

# import filtered data
filtered_data = pd.read_csv('DataFrameFiltered.csv')

# convert to dataframe
df = pd.DataFrame.from_records(filtered_data)

index_to_remove = []
# iterate over every row of dataframe (in this case resembles threads)
for index, row in df.iterrows():
    #count tweets of thread
    tweet_count = 0
    for i in range(len(row)):
        # check if tweet contains something that is no string. If so save thread to be removed later on. Else increase tweet count
        res = isinstance(row[i], str)
        if not res:
            if index not in index_to_remove:
                index_to_remove.append(index)
        else:
            if len(row[i]) != 1:
                tweet_count += 1

df = df.drop(df.index[index_to_remove])

# save new dataframe to specified file
df.to_csv('DataFrameCleaned.csv', index=False)

