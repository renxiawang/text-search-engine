#coding:utf8
import os
from pymongo import Connection
from collections import defaultdict
from colorama import init, Fore, Style, Back
from nltk import cluster
from nltk.corpus import wordnet
from nltk import WordNetLemmatizer
from dicts.sorteddict import ValueSortedDict
import bob
import re
import math
import time
import Tokenizer as tok

def preprocessing(keywords):
	'''Preprocessing the input keywords'''
	global start
	# print "use-preprocessing: ",time.clock()-start
	
	tokens = tok.normalizer(keywords.lower())
	tokens = tok.stop_words(tokens)
	tokens = tok.stemmer(tokens)
	tokens = tok.lemmatizer(tokens)

	return tokens

def extract_docs_ids(docs):
	'''Extract document ids from posting lists'''
	docs_ids = []
	for doc in docs:
		docs_ids.append(doc[0])

	return docs_ids

def intersect_ids(ids):
	'''find documents contain all tokens'''
	common_ids = set(ids[0]) # document id
	for doc in ids:
		common_ids = common_ids.intersection(set(doc))
	
	return common_ids

def is_order_correct(postings_of_tokens):
	'''check if document contains sequence of tokens in correct order'''
	postings_of_tokens = subtraction(postings_of_tokens)

	inter_pos = set(postings_of_tokens[0])
	for i in xrange(1,len(postings_of_tokens)):
		inter_pos = inter_pos.intersection(postings_of_tokens[i])

	if len(inter_pos) != 0:
		return True
	else:
		return False
	

def subtraction(postings):
	'''Do subsection on the positions, 
	except positions of the first token'''
	for i in xrange(1,len(postings)):
		for j in xrange(0,len(postings[i])):
			postings[i][j] = postings[i][j] - i

	return postings

def regular_keywords_query(tokens):
	'''Regular keywords(one or more) query'''
	# print "use-keywords-query: ",time.clock()-start
	docs_ids = set()
	for token in tokens:
		try:
			if dic.has_key(token):
				token_details = dic[token]
				token_postings = token_details['postings']
				token_docs_ids = extract_docs_ids(token_postings) # get relvent document id
				docs_ids = docs_ids.union(set(token_docs_ids)) # duplicates elimination
			else:
				return []
		except:
			print 'wtf'
	
	docs_ids = list(docs_ids)
	result_docs = ranking(tokens, docs_ids)
	return result_docs

def phrase_query(tokens):
	global start
	# print "use-phrase-query: ",time.clock()-start
	
	tokens_postings = [] # postings of all tokens
	ids_in_postings = [] # document ids of all tokens

	for token in tokens:
		if dic.has_key(token) == False:
			return []
		
		token_details = dic[token]
		tokens_postings.append(token_details['postings']) 

		token_docs_ids = extract_docs_ids(token_details['postings'])
		ids_in_postings.append(token_docs_ids)

	common_ids = intersect_ids(ids_in_postings)
	if len(common_ids) == 0:
		# no document contains all query terms
		return []
	
	# get the posting list of tokens in docs
	postings_without_ids = []
	result_docs_ids = []
	for one_id in common_ids:
		for postings_of_a_token in tokens_postings:
			for posting in postings_of_a_token:
				if posting[0] == one_id:
					postings_without_ids.extend(posting[1:])
		
		# check whether tokens are in correct order
		# perform the necessary subtractions
		# intersect the locations
		if is_order_correct(postings_without_ids) == True:
			result_docs_ids.append(one_id)
		
		postings_without_ids = []
	# print result_docs_ids
	result_docs = ranking(tokens, result_docs_ids)
	return result_docs

def get_query_tfidf(doc):
	global start
	# print "use-query-tfidf: ",time.clock()-start
	
	vector = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
	doc_tf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
	doc_idf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
	
	# set tf(without normalization), idf
	for token in doc:
		if doc_tf.has_key(token):
			doc_tf[token] += 1
		doc_idf[token] = dic[token]['idf-value']

	# norm
	norm = 0
	# for token, tf in doc_tf.items():
	# 	if token not in doc:
	# 		continue
	for token in doc:
		norm+=doc_tf[token]**2
	norm = math.sqrt(norm)
	
	# normlize tf
	# for token, tf in doc_tf.items():
	# 	if token not in doc:
	# 		continue
	for token in doc:
		doc_tf[token] = (float(doc_tf[token])/norm)

	# calculate tf-idf
	for token in doc:
		vector[token] = (float(doc_tf[token]) * doc_idf[token])
		
	# for token, tf in doc_tf.items():
	# 	if token not in doc:
	# 		continue
	# 	vector[token] = (float(tf) * doc_idf[token])
	
	return vector.values()

def get_tf(doc_id, token):
	postings = dic[token]['postings']
	for i in xrange(0,len(postings)):
		if postings[i][0] == doc_id:
			return dic[token]['tf-value'][i]

def get_doc_tfidf(docs_ids):
	global start
	# print "use-doc-tfidf: ",time.clock()-start
	
	doc_vectors = {}

	for doc_id in docs_ids:
		# print "use-one-init: ",time.clock()-start
		
		doc = tokens_collection.find({"_id":doc_id})[0]['tokens']

		# print "use-one-init-empty-vector: ",time.clock()-start
		doc_tf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
		doc_idf = dict(zip(dic.keys(), list([0]*len(dic.keys()))))
		vector = []

		# print "use-one-doc-idf: ",time.clock()-start
		
		for token in doc:
			doc_tf[token] = get_tf(doc_id, token)
			doc_idf[token] = dic[token]['idf-value']
		
		# print "use-one-tf-idf: ",time.clock()-start
		
		for token, tf in doc_tf.items():
			vector.append(float(tf) * doc_idf[token])
		
		doc_vectors[doc_id] = vector

	return doc_vectors

def get_doc_tfidf_from_db(docs_ids):
	# print "use-get_doc_tfidf_from_db: ",time.clock()-start
	doc_vectors = {}
	
	# for doc_id in docs_ids:
	# 	# print "use-get_doc_tfidf_from_db-iter: ",time.clock()-start
	# 	vector = []
	# 	vector = map(float, source_collection.find({'meta.newid':doc_id})[0]['vector'].split(',')[:-1])
	# 	doc_vectors[doc_id] = vector
	for doc in source_collection.find({'meta.newid':{'$in':docs_ids}}):
		vector = []
		vector = map(float, doc['vector'].split(',')[:-1])
		doc_vectors[doc['meta']['newid']] = vector

	return doc_vectors
	

def get_top10(dic):
	subdict = {}
	i = 0
	for (k, v) in dic.items():
		if i < 10:
			subdict[k] = v
		else:
			break
		i += 1

	return subdict

def ranking(query, docs_ids):
	global start
	# print "use-ranking: ",time.clock()-start
	
	# query tf-idf
	query_vector = get_query_tfidf(query)
	
	# doc tf-idfs
	# doc_vectors = get_doc_tfidf(docs_ids)
	doc_vectors = get_doc_tfidf_from_db(docs_ids)

	# print "use-cos: ",time.clock()-start
	# cos_simi
	cos_simis = {}
	for (doc_id, vector) in doc_vectors.items():
		# cos_simi = cluster.util.cosine_distance(query_vector, vector)
		cos_simi = bob.math.normalized_scalar_product(query_vector, vector)
		cos_simis[doc_id] = cos_simi

	# print "use-sort: ",time.clock()-start
	# sorting
	sorted_cs = ValueSortedDict(cos_simis, reverse=True)
	# for key, value in sorted(cos_simis.iteritems(), key=lambda (k,v): (v,k), reverse=True):
	# 	sorted_cs[key] = value


	# take top 10
	if len(sorted_cs) >= 10:
		sorted_cs = get_top10(sorted_cs)

	# retrieval source docs
	source_docs = []
	for doc_id in sorted_cs.keys():
		doc = source_collection.find({'meta.newid':doc_id})[0]
		source_docs.append(doc)

	return source_docs

def get_keywords_regx(keywords):
	keywords = keywords.replace('\"', '').split(' ')
	reg = '(?i)('
	for word in keywords:
		reg += word
		reg += '|'
	reg = reg.rstrip('|')
	reg += ')'
	return reg

def summarize(keywords_regx, body):
	sentences = body.split('.')
	matched_sentences = '...'
	num_sentence = 3
	for sentence in sentences:
		if re.search(keywords_regx, sentence) != None:
			if num_sentence == 0:
				break
			sentence = sentence.strip().replace('\n', ' ')
			sentence += '...'
			matched_sentences += sentence
			num_sentence -= 1

	return matched_sentences
	
def main():
	# get query input
	print 'This query engine support 2 types of query'
	print '\t1. regular keyword based queries(one word or more)'
	print '\t2. phrase queries(e.g. "computer science")'
	print '\tEnter -1 to exit'
	while True:
		keywords = ''
		keywords = raw_input('Please enter your query: ')
		if keywords == '-1':
			break
		result_docs =[]
		tokens = []
		# detect query type
		global start
		# print "use-start: ",time.clock()-start
		
		if keywords[0] == "\"" and keywords[-1] == "\"":
			# phrase query
			tokens = preprocessing(keywords)
			result_docs = phrase_query(tokens)
		else:
			# regular keyword based queries(one word or more)
			tokens = preprocessing(keywords)
			result_docs = regular_keywords_query(tokens)

		# print "use-display: ",time.clock()-start
		
		# display query result
		print '\n************************************************'
		print 'Search results for ', Fore.BLUE, keywords, Fore.RESET
		print '************************************************'
		if len(result_docs) == 0:
			print 'No record'
		else:
			for doc in result_docs:
				regx = get_keywords_regx(keywords)
				print Style.BRIGHT, "Title: \n", Style.RESET_ALL, re.sub(regx, Fore.RED + r'\1' + Fore.RESET, Style.BRIGHT + Back.BLUE + doc['title'].strip() + Style.RESET_ALL)
				print Style.BRIGHT, "Summary: \n", Style.RESET_ALL, re.sub(regx, Fore.RED + r'\1' + Fore.RESET, summarize(regx, doc['body'].strip()))
				print '------------------------------------------------------\n'

start = time.clock()
# print "use-init: ",time.clock()-start

# init NLTK
WordNetLemmatizer().lemmatize("")
# connect db
db_connect = Connection('127.0.0.1', 27017)
db = db_connect['search_engine']
index_collection = db['index']
source_collection = db['source']
source_bk_collection = db['source_bk']
tokens_collection = db['tokens']

# statistic
dic = {}
indics = index_collection.find()

# print "use-load-index: ",time.clock()-start
for index in indics:
	tmp = {}
	tmp['tf-value'] = index['tf-value']
	tmp['idf-value'] = index['idf-value']
	tmp['postings'] = index[index['processed-token']]
	dic[index['processed-token']] = tmp
	# {u'cmph': {'tf': [u'0.2764', u'-0.0187'], 'idf': u'4.0330', 'postings': [[u'7519', [4]], [u'9111', [4]]]}}

if __name__ == '__main__':
	main()