import pandas as pd
from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
import json

pd.options.display.float_format = '{:.2f}'.format


es = Elasticsearch("http://localhost:9200")



def cleanUserID(userID):
	userID = userID.replace('=', '')
	return userID


def dateConvert(value):
	value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
	return value


def processTipFrame(tipFrame):
	tipFrame['user_id'] = tipFrame['user_id'].apply(cleanUserID)
	tipFrame['date'] = tipFrame['date'].apply(dateConvert)
	tipFrame.rename(columns={'date':'datetime'}, inplace=True)
	return tipFrame


def insertTip():
	df = pd.read_csv(r"D:\Projects\YELP\data\tip.csv")
	
	if es.indices.exists(index='tip_user'):
		es.indices.delete(index='tip_user')
		print('Deleted')

	configurations = {
	"mappings":{
		"properties":{
				"datetime":{"type":"date"},
			}
		}
	}

	es.indices.create(index="tip_user", body=configurations)

	df = processTipFrame(df)
	
	json_str = df.to_json(orient='records', date_format='iso')
	json_records = json.loads(json_str)

	action_list = []
	for row in json_records:
		record ={
			'_op_type': 'index',
			'_index': 'tip_user',
			'_source': row
		}
		action_list.append(record)

	helpers.bulk(es, action_list, raise_on_error=False)






def insertReviews():
	if es.indices.exists(index='reviews'):
		es.indices.delete(index='reviews')
		print('Deleted')

	es.indices.create(index="reviews")

	df = pd.read_csv(r"D:\Projects\YELP\data\review.csv")
	df = df[['review_id', 'text']]
	df.rename(columns={'text':'review'}, inplace=True)

	print('Loaded and renamed')



	json_str = df.to_json(orient='records')
	json_records = json.loads(json_str)

	print('df converted to json')

	action_list = []
	for row in json_records:
		record ={
			'_op_type': 'index',
			'_index': 'rewiews',
			'_source': row
		}
		action_list.append(record)

	print('json converted to list and now inserting')

	helpers.bulk(es, action_list, raise_on_error=False)



insertReviews()

# insertTip()
# es.indices.create(index="test")


