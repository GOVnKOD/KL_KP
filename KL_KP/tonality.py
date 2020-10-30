import sys
import os.path
import pymongo
from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel

db = None;
sentencesWithPerson = None;
sentencesWithPlaces = None;

def connectToDB():        
    
    client = pymongo.MongoClient("mongodb://govnkod:govnkod47Top@maincluster-shard-00-00.9aegl.gcp.mongodb.net:27017,maincluster-shard-00-01.9aegl.gcp.mongodb.net:27017,maincluster-shard-00-02.9aegl.gcp.mongodb.net:27017/newsDB?ssl=true&replicaSet=mainCluster-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = client.newsDB
    try:
        print("MongoDB version is %s" %
            client.server_info()['version'])
        return db;
    except pymongo.errors.OperationFailure as error:
        print(error);


def tokenizeByText(text: str):
    messages = [text]
    res = analyze(messages)
    #print(res)
    return res

def predict(words):
    return model.predict(words, k=4)

def analyze(text: list):
    results = predict(text)
    return results[0]

tokenizer = RegexTokenizer()
model = FastTextSocialNetworkModel(tokenizer=tokenizer)

def getNotAnalyzedData(pTable):
    return pTable.find({"isAnalyzed":False})

def analyzeData(cursor,table):    
    for i in cursor:
        text = i['sentences']
        res= tokenizeByText(text)
        id = i['_id']
        table.update_one({"_id":id},{"$set":{'tonality':res,
                                             'isAnalyzed':True}});
    
if __name__ == "__main__":
    db = connectToDB();
    sentencesWithPerson = db.sentencesWithPerson;
    sentencesWithPlaces = db.sentencesWithPlaces;
    notAnalyzedSentencesP = getNotAnalyzedData(sentencesWithPerson);
    notAnalyzedSentencesPl = getNotAnalyzedData(sentencesWithPlaces);
    analyzeData(notAnalyzedSentencesP, sentencesWithPerson);
    analyzeData(notAnalyzedSentencesPl, sentencesWithPlaces);
    print('Усе готово шеф!')
    