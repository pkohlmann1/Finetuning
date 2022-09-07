import pandas as pd
import advertools as adv
import os


threads = {}
keyCounter = 0


# Method which generates threads out of all the tweets in the dataframe
def generate_threads():
    global threads
    # loop over tweets that start a thread
    starting_tweets = dataFrame.loc[dataFrame['in_response_to_tweet_id'] == ""]
    for dfRow in starting_tweets.iterrows():
        threadString = "" + dfRow[1]['text']
        # ignore starting tweets that contain some form of direct message reference
        if not "DM" in threadString and not "Dm" in threadString \
                and not "direct message" in threadString and not "Direct Message" in threadString \
                and not "private message" in threadString and not "Private Message" in threadString:
            generate_thread_response_tree(dfRow[1], threadString)


# recursively build the thread from the starting tweet
def generate_thread_response_tree(row, threadString):
    global keyCounter
    global dataFrame
    # check if tweet has responses
    if row['response_tweet_id'] != "":
        # loop over responses
        for responseTweetId in row['response_tweet_id'].split(","):
            # check if tweet with same id as response tweet exists
            if int(responseTweetId) in dataFrame.index:
                # get response tweet, build tweet string and continue to build thread
                response = dataFrame.loc[[int(responseTweetId)]]
                response = response.squeeze()
                text = "" + response['text']
                if not response.empty and not "DM" in text and not "Dm" in text\
                and not "direct message" in text and not "Direct Message" in text\
                and not "private message" in text and not "Private Message" in text:
                    newThreadString = threadString + " /// " + response['text']
                    generate_thread_response_tree(response, newThreadString)
    # thread finished building
    else:
        threadString = filter_last_response_if_inbound(row, threadString)
        threads[keyCounter] = threadString
        keyCounter += 1
        if keyCounter % 100000 == 0:
            print(str(keyCounter) + " Threads generiert")


# We can not use threads that end with a customer response so cut the last tweet if it belongs to a customer (inbound)
def filter_last_response_if_inbound(row, threadString):
    if row["inbound"]:
        tweets = threadString.split(" /// ")
        tweets.pop()
        threadStringWithoutLastResponse = ""
        for tweet in tweets:
            threadStringWithoutLastResponse += tweet + " /// "
        threadString = threadStringWithoutLastResponse
        threadString = threadString[:len(threadString) - 3]
    return threadString


# We do not want duplicate threads
def filter_duplicates():
    global threads
    temp = {val: key for key, val in threads.items()}
    res = {val: key for key, val in temp.items()}
    threads = res


# We do not want threads with only a single tweet
def filter_single_tweet_threads():
    for key in list(threads.keys()):
        if "///" not in threads[key]:
            del threads[key]


# We do not want emojis in the tweets
def filter_emojis():
    c = 0
    for key in list(threads.keys()):
        if c % 1000 == 0:
            print(str(c) + " Threads von Emojis gefiltert")
        summary = adv.extract_emoji(threads[key])
        if len(summary["emoji_flat"]) > 0:
            if '🤦' in summary["emoji_flat"]:
                del threads[key]
            else:
                for emoji in summary["emoji_flat"]:
                    threads[key] = threads[key].replace(emoji, "")
        c += 1


# We do not want threads that contain links
def filter_links():
    for key in list(threads.keys()):
        if (("https" in threads[key]) or ("http" in threads[key])):
            del threads[key]


# Filter special symbols
def filter_at_tringales_amp_star_hash_minus():
    for key in threads:
        listOfWords = threads[key].split()
        newlist = []
        for word in listOfWords:
            if not (word[0] == "@" or word[0] == "^" or word[0] == "*" or word[0] == "#" or word[0] == "-" or word[0] == "." or ("&amp" in word)):
                newlist.append(word)
            if (word[0] == "&"):
                newlist.append("and")
            if ("&amp" in word):
                newlist.append(word[:-4])
        threadWithoutChars = ' '.join(newlist)
        threads[key] = threadWithoutChars


# folder path to all the tweets stored as multiple chunks
dir_path = r'CHUNKS_UNUSED'

# list to store files
res = []

# Iterate directory
for path in os.listdir(dir_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(dir_path, path)):
        res.append('CHUNKS_UNUSED/' + path)

# merge files
dataFrame = pd.concat(
   map(pd.read_csv, res), ignore_index=True)

# fill empty cell values and drop not needed column
dataFrame.fillna('', inplace=True)
dataFrame.drop(columns=['created_at'], inplace=True)
# reformating column values
dataFrame['in_response_to_tweet_id'] = dataFrame['in_response_to_tweet_id'].map(lambda in_response_to_tweet_id: "" if in_response_to_tweet_id == '' else str(int(in_response_to_tweet_id)))
# setting tweet_ids as dataframe index for index-access to enable fast performance
dataFrame.set_index('tweet_id', inplace=True, drop=False)


print("make threads")
generate_threads()
print("Number of Threads: " + str(len(threads)))

print("filtering duplicates")
filter_duplicates()
print("Number of Threads: " + str(len(threads)))

print("filtering sigle tweets")
filter_single_tweet_threads()
print("Number of Threads: " + str(len(threads)))

print("filtering Emojis")
filter_emojis()
print("Number of Threads: " + str(len(threads)))

print("filtering links")
filter_links()
print("Number of Threads: " + str(len(threads)))

print("filtering symbols")
filter_at_tringales_amp_star_hash_minus()
print("Number of Threads: " + str(len(threads)))


lengthOfThreads = threads.__len__()
print("threads available: ", lengthOfThreads)


finalDataFrame = pd.DataFrame(
    columns=['response', 'context', 'context/0', 'context/1', 'context/2', 'context/3', 'context/4', 'context/5'])

for i in range(lengthOfThreads):
    finalDataFrame.loc[i] = [" ", " ", " ", " ", " ", " ", " ", " "]
print("created empty DF")
print("fill DF with threads")
# limit thread length to 8 tweets and parse thread dict back into dataframe in correct order
c = 0
for thread in threads:
    tweets = threads[thread]
    listOfTwets = tweets.split("///")
    lengthOfthread = len(listOfTwets)
    if (lengthOfthread > 8):
        # cut of everything behind 8th tweet
        listOfTwets = listOfTwets[-8:]
        lengthOfthread = len(listOfTwets)
    for tweet in listOfTwets:
        finalDataFrame.iloc[c, (lengthOfthread - 1)] = tweet
        lengthOfthread -= 1
    c += 1


print("last filter")
for i in range(finalDataFrame.shape[0]):
    temp = finalDataFrame.iat[i,0]
    temp = temp[:-1]
    finalDataFrame.iat[i, 0] = temp


finalDataFrame.to_csv('DataFrameFiltered.csv', index=False)
print("wrote DF to disc")