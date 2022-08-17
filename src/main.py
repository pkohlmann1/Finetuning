import pandas as pd
import csv
import advertools as adv
import json
# import pycld2 as cld2

threads = {}
keyCounter = 0

def generate_threads():
    global threads
    for dfRow in myDF.iterrows():
        dfRow = dfRow[1]
        if dfRow['in_response_to_tweet_id'] == "":
            threadString = "" + dfRow['text']
            generate_thread_response_tree(dfRow, threadString)
    threads = filter_duplicates()
    # with open('file_generateThreads.txt', 'w') as file:
    #     file.write(json.dumps(threads))  # use `json.loads` to do the reverse

def generate_thread_response_tree(row, threadString):
    global keyCounter
    if not row['response_tweet_id'] == "":
        for responseTweetId in row['response_tweet_id'].split(","):
            response = myDF.loc[myDF['tweet_id'] == responseTweetId]
            if response.empty:
                pass
            else:
                response = response.squeeze()
                newThreadString = threadString + " /// " + response['text']
                generate_thread_response_tree(response.squeeze(), newThreadString)
    else:
        threadString = filter_last_response_if_inbound(row, threadString)
        threads[keyCounter] = threadString
        keyCounter += 1

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

def filter_duplicates():
    result = {}
    for key, value in threads.items():
        if value not in result.values():
            result[key] = value
    return result

def filter_single_tweet_threads():
    for key in list(threads.keys()):
        if "///" not in threads[key]:
            del threads[key]

def filter_emojis():
    for key in list(threads.keys()):
        summary = adv.extract_emoji(threads[key])
        if len(summary["emoji_flat"]) > 0:
            if '🤦' in summary["emoji_flat"]:
                del threads[key]
            else:
                for emoji in summary["emoji_flat"]:
                    threads[key] = threads[key].replace(emoji, "")

def filter_links():
    for key in list(threads.keys()):
        if (("https" in threads[key]) or ("http" in threads[key])):
            del threads[key]

# def filter_language():
#     for key in list(threads.keys()):
#         _, _, _, lang = cld2.detect(threads[key], returnVectors=True)
#         if (lang[0][3] != "en"):
#             del threads[key]

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


myDF = pd.DataFrame(
    columns=['tweet_id', 'author_id', 'inbound', 'text', 'response_tweet_id', 'in_response_to_tweet_id'])

with open('CHUNK1.csv', newline='', encoding="mbcs") as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    counter = 0
    for row in reader:
        myDF.loc[counter] = [row[0], row[1], row[2], row[4], row[5], row[6]]
        counter += 1

generate_threads()
print("made threads")

filter_single_tweet_threads()
print("filtered sigle tweets")
# with open('file_filterSingle.txt', 'w') as file:
#     file.write(json.dumps(threads))

filter_emojis()
print("filtered Emojis")
# with open('file_emojis.txt', 'w') as file:
#     file.write(json.dumps(threads))

filter_links()
print("filterd links")
# with open('file_links.txt', 'w') as file:
#     file.write(json.dumps(threads))

# Commented out because package is not working on my local machine
# filter_language()
# print("filtered language")

filter_at_tringales_amp_star_hash_minus()
print("filtered symbols")
# with open('file_symbols.txt', 'w') as file:
#     file.write(json.dumps(threads))

cntr = 0
lengthOfThreads = threads.__len__()
print("threads available: ", lengthOfThreads)


finalDataFrame = pd.DataFrame(
    columns=['response', 'context', 'context/0', 'context/1', 'context/2', 'context/3', 'context/4', 'context/5'])

for i in range(lengthOfThreads):
    finalDataFrame.loc[i] = [" ", " ", " ", " ", " ", " ", " ", " "]
print("created empty DF")

positionOfRow = 0
for thread in threads:
    tweets = threads[thread]
    listOfTwets = tweets.split("///")
    lengthOfthread = len(listOfTwets)
    if (lengthOfthread > 8):
        listOfTwets = listOfTwets[-8:]
        lengthOfthread = len(listOfTwets)
    for tweet in listOfTwets:
        positionOfColumn = lengthOfthread
        finalDataFrame.iloc[positionOfRow, (lengthOfthread - 1)] = tweet
        lengthOfthread -= 1
    positionOfRow += 1

print("filled DF")

for i in range(finalDataFrame.shape[0]):
    temp = finalDataFrame.iat[i,0]
    temp = temp[:-1]
    finalDataFrame.iat[i, 0] = temp

print("last filter")


finalDataFrame.to_csv('DataFrame1.csv', index=False)
print("wrote DF to disc")