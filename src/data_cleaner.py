import pandas as pd

THREAD_LENGTH = 4

# import filtered data
filtered_data = pd.read_csv('DataFrame1.csv')

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
    # only keep threads with exactly the specified thread_length
    # if tweet_count != THREAD_LENGTH:
    #     index_to_remove.append(index)

# drop last three columns of each row of dataframe because they are for some reason empty
df = df.drop(df.index[index_to_remove])
# df = df.iloc[: , :-3]

# save new dataframe to specified file
df.to_csv('DataFrame6.csv', index=False)

