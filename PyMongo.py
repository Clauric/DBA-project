import pymongo as pmd
from pmd import MongoClient

client = MongoClient("Localhost",27017)				# Set Mongo Client to local hose

db = client.Assignment								# Set db as the DB required
collection = db.project								# Set the collection to the collection required

dm_C = pd.DataFrame(list(collection.find()))		# Convert mongoDB in dataframe

print(dm_C)

