import pymongo
import json
from pymongo import MongoClient, InsertOne

CONNECTION_STRING = "mongodb://user123:pass123@localhost:27017/"
DATABASE = "schema"
COLLECTION = "schema"
FILENAME = "config/schema.json"

client = pymongo.MongoClient(CONNECTION_STRING)
db = client[DATABASE]
collection = db[COLLECTION]
requesting = []

with open(FILENAME, 'r', encoding='utf-8') as f:
    data = json.load(f)
    for key, value in data.items():
        document = value
        document["_id"] = key
        requesting.append(InsertOne(document))


result = collection.bulk_write(requesting)
client.close()
