#coding:utf8
from pymongo import Connection
from collections import defaultdict
import math

def main():
    # connect the database
    dbConnect = Connection('127.0.0.1', 27017)
    db = dbConnect['search_engine']
    tokensCollection = db['tokens']
    indexCollection = db['index']

    #used for main inverted index
    dic = {}
    tf = defaultdict(list)
    df = defaultdict(int)
    numDoc = 0

    #get articles from database
    for article in tokensCollection.find():
        _id = article.get('_id')
        tokens = article.get('tokens')
        numDoc += 1
        postingList = {}

        #construct posting list for current article
        for index, token in enumerate(tokens):
            if not postingList.get(token):
                postingList[token] = [_id, [index]]
            else:
                postingList[token][1].append(index)

        # normalize the document vector
        norm = 0
        for token, posting in postingList.iteritems():
            norm += len(posting[1])**2
        norm = math.sqrt(norm)

        # calculate the tf and df weights
        for token, posting in postingList.iteritems():
            tf[token].append(len(posting[1])/norm)
            df[token]+=1

        # Merge the current article index with the main inverted index
        for key, value in postingList.items():
            dic.setdefault(key, []).append(value)

    #insert the main inverted index into database
    for key,value in dic.items():
        idf = math.log10(float(numDoc)/df[key])
        indexCollection.insert({key:value, 'tf-value':tf[key], 'idf-value':idf, 'processed-token':key})

if __name__ == '__main__':
    main()
