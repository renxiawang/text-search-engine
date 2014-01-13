#coding:utf8

from pymongo import Connection
from collections import defaultdict
import math
import nltk

def get_tf(doc_id, token):
	postings = dic[token]['postings']
	for i in xrange(0,len(postings)):
		if postings[i][0] == doc_id:
			return dic[token]['tf-value'][i]

# connect db
db_connect = Connection('127.0.0.1', 27017)
db = db_connect['search_engine']
index_collection = db['index']
source_collection = db['source']
tokens_collection = db['tokens']

# load indices and build dict
dic = {}
indics = index_collection.find()

for index in indics:
	tmp = {}
	tmp['tf-value'] = index['tf-value']
	tmp['idf-value'] = index['idf-value']
	tmp['postings'] = index[index['processed-token']]
	dic[index['processed-token']] = tmp
	# {u'cmph': {'tf': [u'0.2764', u'-0.0187'], 'idf': u'4.0330', 'postings': [[u'7519', [4]], [u'9111', [4]]]}}

# calculate docs vector
i = 0
for doc in tokens_collection.find(timeout=False):
	i += 1
	doc_id = doc['_id']
	print 'processing doc ', i, ' ', doc_id
	tokens = doc['tokens']

	doc_tf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
	doc_idf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
	
	vector = ''
	for token in tokens:
		doc_tf[token] = get_tf(doc_id, token)
		doc_idf[token] = dic[token]['idf-value']
		
	for token, tf in doc_tf.items():
		# vector.append(float(tf) * doc_idf[token])
		vector = vector + str(float(tf) * doc_idf[token]) + ','
	
	source_collection.update({'meta.newid':doc_id}, {'$set':{'vector':vector}})
