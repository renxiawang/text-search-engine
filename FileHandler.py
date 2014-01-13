#coding:utf8
import os
from pyquery import PyQuery 
from bs4 import BeautifulSoup
from pymongo import Connection
from bson.objectid import ObjectId
from bson.son import SON

# connect mongodb
dbConnect = Connection('127.0.0.1', 27017)
db = dbConnect['search_engine']
collect_source = db['source']

# read files
dataFilePath = os.getcwd() + '/data/'
dataFiles = [x for x in os.listdir(dataFilePath) if x.endswith('sgm')] 

for dataFile in dataFiles:
    fp = open(dataFilePath+dataFile)
    soup = BeautifulSoup(fp.read())
    fp.close()
    articles = soup.find_all('reuters')
    for article in articles:
        dic = {}
        dic['meta'] = {}
        try:
            topicsAttr = article['topics']
            dic['meta']['topics'] = topicsAttr
        except:
            pass
        try:
            lewissplitAttr = article['lewissplit']
            dic['meta']['lewissplit'] = lewissplitAttr
        except:
            pass
        try:
            cgisplitAttr = article['cgisplit']
            dic['meta']['cgisplit'] = cgisplitAttr
        except:
            pass
        try:
            oldidAttr = article['oldid']
            dic['meta']['oldid'] = oldidAttr
        except:
            pass
        try:
            newidAttr = article['newid']
            dic['meta']['newid'] = newidAttr
        except:
            pass
        try:
            date = article.date.text
            dic['date'] = date
        except:
            pass
        try:
            topics = article.topics.text
            dic['topics'] = topics
        except:
            pass
        try:
            places = article.places.text
            dic['places'] = places
        except:
            pass
        try:
            d = article.d.text
            dic['d'] = d
        except:
            pass
        try:
            people = article.people.text
            dic['people'] = people
        except:
            pass
        try:
            orgs = article.orgs.text
            dic['orgs'] = orgs
        except:
            pass
        try:
            exchanges = article.exchanges.text
            dic['exchanges'] = exchanges
        except:
            pass
        try:
            companies = article.companies.text
            dic['companies'] = companies
        except:
            pass
        try:
            unknown = article.unknown.text
            dic['unknown'] = unknown
        except:
            pass
        try:
            title = article.title.extract().text
            dic['title'] = title
        except:
            pass
        try:
            dateline = article.dateline.extract().text
            dic['dateline'] = dateline
        except:
            pass
        try:
            body = article.find('text').text
            dic['body'] = body
        except:
            pass
        collect_source.insert(dic)
        # print dic
    

        