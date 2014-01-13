#coding:utf8
import nltk
from nltk.corpus import stopwords as sp
from nltk import PorterStemmer
from nltk import WordNetLemmatizer
from nltk.corpus import wordnet
from pymongo import Connection
from bson.objectid import ObjectId
from bson.son import SON
import re

def normalizer(text):
    del_reg = r"((1st|2nd|3rd|[0-9]+th)|(\$?\d+(\.\d+)?%?)|(\d+(\.\d+)?)|([][\.,;\"\'\?():\-_`])|(\W))"
    text = re.sub(del_reg, " ", text)
    tokens = nltk.word_tokenize(text)
    return tokens

def stop_words(tokens):
	stopwords = sp.words('english')
	text = [w for w in tokens if w.lower() not in stopwords]
	return text

def stemmer(tokens):
	porter = PorterStemmer()
	text = [porter.stem(t) for t in tokens]
	return text

def lemmatizer(tokens):
	wnl = WordNetLemmatizer()
	text = [wnl.lemmatize(t) for t in tokens]
	return text

def main():
	# connect the database
	dbConnect = Connection('127.0.0.1', 27017)
	db = dbConnect['search_engine']
	collect_source = db['source']
	collect_target = db['tokens_test']

	# get source articles
	articles = collect_source.find()
	for article in articles:
		# get id and text
		new_id = article['meta']['newid']
		try:
			text = article['title']
		except:
			pass
		try:
			text = text + article['body']
		except:
			pass

		# normalizer
		tokens = normalizer(text.lower())

		# delete stop words
		tokens = stop_words(tokens)

		# stemming
		tokens = stemmer(tokens)

		# lemmatizing
		tokens = lemmatizer(tokens)

		dic = dict()
		dic['_id'] = new_id
		dic['tokens'] = tokens

		# insert into database
		collect_target.insert(dic)

if __name__ == '__main__':
	main()
