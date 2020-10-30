from threading import Thread
from time import sleep
from bs4 import BeautifulSoup
import pymongo
import requests as req
import keyboard  # using module keyboard
import datetime
import os
import subprocess as sub
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Word2Vec


db = None;
News = None;
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

def getNotAnalyzedNews():
    return News.find({"isAnalyzed":False})

def writeAnalyzedSentencesToDB(outputP,outputPl,id):
    a=0;
    itemsP = []
    itemsPl=[]
    for item in outputP:
        sentencesWithPersonI = {
            "newsId" : id,
            "sentences":item,
            "isAnalyzed":False
            }
        itemsP.append(sentencesWithPersonI)
        
    for item in outputPl:
        sentencesWithPlacesI = {
            "newsId" : id,
            "sentences":item,
            "isAnalyzed":False
            }
        itemsPl.append(sentencesWithPlacesI);
    if len(itemsP)>0:
        sentencesWithPerson.insert_many(itemsP)
    if len(itemsPl)>0:
        sentencesWithPlaces.insert_many(itemsPl)
    News.update_one({"_id":id},{"$set":{'isAnalyzed':True}});


def analyze(notAnalyzedNews):
    for news in notAnalyzedNews:
        text = news['text'];
        with open(os.path.join(os.getcwd(), 'input.txt'), 'w', encoding='utf-8') as inputFile:
            inputFile.writelines(text)

        p = sub.Popen(["tomita-parser", "persons.proto"], stdout=sub.PIPE, stderr=sub.PIPE)
        out, err = p.communicate()
        print(out, err)
        outputP = []
        outputPl = []
        with open(os.path.join(os.getcwd(), 'output.txt'), 'r', encoding='utf-8') as outputFile:
            readedNews = outputFile.readlines()
            i=0;
            while i <len(readedNews)-2:
                analyzedsentences= readedNews[i];
                findFact = readedNews[i+1].find("Fact")
                if (findFact!=-1):
                    if(readedNews[i+3][readedNews[i+2].find("=")+2]!="\n"):
                        outputP.append(analyzedsentences);
                    if(readedNews[i+4][readedNews[i+3].find("=")+2]!="\n"):
                        outputPl.append(analyzedsentences);
                    i+=6;
                else:
                    i+=1;
            writeAnalyzedSentencesToDB(outputP,outputPl,news['_id']);

def makeWord2VecModel():
    cursor = News.find({});
    text ="";
    for news in cursor:
        text +=news['text'];
    with open(os.path.join(os.getcwd(), 'word2Vec.txt'), 'w', encoding='utf-8') as inputFile:
            inputFile.writelines(text)
    spark = SparkSession.builder.appName("SimpleApplication").getOrCreate()

    # Построчная загрузка файла в RDD
    input_file = spark.sparkContext.textFile('word2Vec.txt')
    
    print(input_file.collect())
    prepared = input_file.map(lambda x: ([x]))
    df = prepared.toDF()
    prepared_df = df.selectExpr('_1 as text')
    
    # Разбить на токены
    tokenizer = Tokenizer(inputCol='text', outputCol='words')
    words = tokenizer.transform(prepared_df)
    
    # Удалить стоп-слова
    stop_words = StopWordsRemover.loadDefaultStopWords('russian')
    remover = StopWordsRemover(inputCol='words', outputCol='filtered', stopWords=stop_words)
    filtered = remover.transform(words)
    
    # Вывести стоп-слова для русского языка
    print(stop_words)
    
    # Вывести таблицу filtered
    filtered.show()
    
    # Вывести столбец таблицы words с токенами до удаления стоп-слов
    words.select('words').show(truncate=False, vertical=True)
    
    # Вывести столбец "filtered" таблицы filtered с токенами после удаления стоп-слов
    filtered.select('filtered').show(truncate=False, vertical=True)
    
    # Посчитать значения TF
    vectorizer = CountVectorizer(inputCol='filtered', outputCol='raw_features').fit(filtered)
    featurized_data = vectorizer.transform(filtered)
    featurized_data.cache()
    vocabulary = vectorizer.vocabulary
    
    # Вывести таблицу со значениями частоты встречаемости термов.
    featurized_data.show()
    
    # Вывести столбец "raw_features" таблицы featurized_data
    featurized_data.select('raw_features').show(truncate=False, vertical=True)
    
    # Вывести список термов в словаре
    print(vocabulary)
    
    
    # Посчитать значения DF
    idf = IDF(inputCol='raw_features', outputCol='features')
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)
    
    # Вывести таблицу rescaled_data
    rescaled_data.show()
    
    # Вывести столбец "features" таблицы featurized_data
    rescaled_data.select('features').show(truncate=False, vertical=True)
    
    # Построить модель Word2Vec
    word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol='words', outputCol='result')
    model = word2Vec.fit(words)
    w2v_df = model.transform(words)
    w2v_df.show()
    persons = []

    cPersons = db.Persones.find({})
    for secName in cPersons:
        persons.append(secName['sName']);
    
    synonyms = []
    i=0;
    synonyms.append(model.findSynonyms('погибла', 2))
    
    for word, cosine_distance in synonyms:
        print (str(word))
    
    spark.stop()



def timerScript(deltaTime):
    ourTime=0;
    parseThread = None;
    while threadState:
        if ourTime==0:
            #notAnalyzedNews = getNotAnalyzedNews();
            #analyze(notAnalyzedNews);
            makeWord2VecModel();
            print("News are analyzed!");
        elif ourTime>0 and ourTime<deltaTime:
            ourTime+=1;
        else:
            ourTime=0;
        sleep(1);

def waitExitKey():
    while True:  # making a loop       
        try:  # used try so that if user pressed other than the given key error will not be shown
            if keyboard.is_pressed('~'):  # if key 'enter' is pressed                 
                break  # finishing the loop
            else:
                pass
        except:
            break  


if __name__ == "__main__":
    db = connectToDB();
    News = db.News;
    sentencesWithPerson = db.sentencesWithPerson;
    sentencesWithPlaces = db.sentencesWithPlaces;
    timerThread = Thread(target = timerScript, args=(60,));
    threadState = 1;
    timerThread.start();
    waitExitKey();
    threadState = 0;
    timerThread.join();