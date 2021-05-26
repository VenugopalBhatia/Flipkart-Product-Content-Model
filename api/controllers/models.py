import pymongo

class connect:
    def __init__(self,db_name):
        self.db = db_name
    
    def createConnection(self,collection_name):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[self.db]
        collection_ = db[collection_name]
        return collection_