from threading import Thread
from time import sleep
from bs4 import BeautifulSoup
import pymongo
import requests as req
import keyboard  # using module keyboard
import datetime

#from RamazanPlayer import *

threadState = 0;

db = None;
News = None;

url = "https://www.volgograd.kp.ru/online/";

def changeDateToISO(date):
    newstr = "";
    if date.find("января")>0:
        newstr = date.replace("января","Jan");
    if date.find("февраля")>0:
        newstr = date.replace("февраля","Feb");
    if date.find("марта")>0:
        newstr = date.replace("марта","Mar");
    if date.find("апреля")>0:
        newstr = date.replace("апреля","Apr");
    if date.find("мая")>0:
        newstr = date.replace("мая","May");
    if date.find("июня")>0:
        newstr = date.replace("июня","Jun");
    if date.find("июля")>0:
        newstr = date.replace("июля","Jul");
    if date.find("августа")>0:
        newstr = date.replace("августа","Aug");
    if date.find("сентября")>0:
        newstr = date.replace("сентября","Sep");
    if date.find("октября")>0:
        newstr = date.replace("октября","Oct");
    if date.find("ноября")>0:
        newstr = date.replace("ноября","Nov");
    if date.find("декабря")>0:
        newstr = date.replace("декабря","Dec");
    return newstr;


def getHtml(url):
    return req.get(url).text;

def getNewsUrls(html):
    soup = BeautifulSoup(html,'lxml');
    allItemWrappers = soup.find_all('div', class_ = 'styled__DigestItemWrapper-sc-5215n1-16 gCgjYC');
    newsUrls = [];
    for itemWrapper in allItemWrappers:
        temp = itemWrapper.find_all('a');
        result = temp[1].get('href').replace('/online/','');
        newsUrls.append(url+result);
        a=0;
    return newsUrls;


def connectToDB():        
    
    client = pymongo.MongoClient("mongodb://govnkod:govnkod47Top@maincluster-shard-00-00.9aegl.gcp.mongodb.net:27017,maincluster-shard-00-01.9aegl.gcp.mongodb.net:27017,maincluster-shard-00-02.9aegl.gcp.mongodb.net:27017/newsDB?ssl=true&replicaSet=mainCluster-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = client.newsDB
    try:
        print("MongoDB version is %s" %
            client.server_info()['version'])
        return db;
    except pymongo.errors.OperationFailure as error:
        print(error);

def getNameOfNews(soup):
    nameOfNews = soup.find('h1', class_ = 'styled__Heading-sc-1futiat-3 jyKuBq');
    temp = nameOfNews.text.strip();
    return temp;

def getContainsOfNews (soup):
    a=0;
    parsedText=soup.find_all('p', class_ = 'styled__Paragraph-sc-17amg0v-0 deTQca');
    text = "";
    for tempText in parsedText:
        temp = tempText.text.strip();
        text+=temp;
    return text;

def getDateOfNews(soup):
    date = soup.find('time',class_ = "styled__Time-sc-1futiat-1 mdIfz").text.strip();
    nDate = changeDateToISO(date);
    date = datetime.datetime.strptime(nDate,'%d %b %Y %H:%M')
    return date;

def getCommentsCount(soup):
    count=0;
    buttonText = soup.find('span',class_ = "styled__Text-sc-1rk2kro-0 styled__RegularText-sc-1rk2kro-1 gUvVCV").text.strip();
    if buttonText == "Комментировать":
        return count;
    else:
        buttonText = buttonText.replace("Комментарии (",'');
        buttonText = buttonText.replace(")",'');
        count = int(buttonText);
        return count;


def parseToDB(url):
    tHtml=getHtml(url);
    soup = BeautifulSoup(tHtml,'lxml');
    name = getNameOfNews(soup);
    contains = getContainsOfNews(soup);
    date = getDateOfNews(soup);
    commentsCount = getCommentsCount(soup);
    myNews = {
        "url":url,
        "name": name,
        "date": date,
        "text":contains,    
        "comCount":commentsCount,
        "isAnalyzed":False
        }
    News_id = News.insert_one(myNews).inserted_id
    a=0;
    

def DBContainsUrl(newsUrl):
    try:
        cursor = News.find_one({"url": newsUrl})
        if cursor:
            return True;
        else:
            return False;
    except BaseException:
        return False;

def tryAddToDB(newsUrl):
    if DBContainsUrl(newsUrl):
        return False;
    else:
        parseToDB(newsUrl);
        return True;


def parseNews(url):
    startHtml = getHtml(url);
    newsUrls = getNewsUrls(startHtml);
    a=0;
    for newsUrl in newsUrls:
       tryAddToDB(newsUrl);
    print("Новости спарсил, милорд!");


    

def timerScript(deltaTime):
    ourTime=0;
    parseThread = None;
    while threadState:
        if ourTime==0:
            parseThread = Thread( target = parseNews, args =(url,));
            parseThread.start();
            print("тут Я должен пурсить");
            ourTime+=1;
            parseThread.join()
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
    timerThread = Thread(target = timerScript, args=(60,));
    threadState = 1;
    timerThread.start();
    waitExitKey();
    threadState = 0;
    timerThread.join();
    
