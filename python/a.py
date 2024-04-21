import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

import concurrent.futures
import pandas as pd
import xlwings as xw
import json
import difflib 
import threading
import time
from lxml import etree,objectify
totalerr=""
import sys 
from operator import length_hint 
import dictdiffer 
from collections import OrderedDict
import numpy as np

# for now()
import datetime
 
# for timezone()
import pytz


def writeTojson(fname, data):
    try:        
        with open(fname, 'w') as convert_file: 
            convert_file.write(json.dumps(data))
    except:
        print(fname ,'failed')
     
    return 

def processdbdiff(tab2dbnamevaluelist,tab2dbnamekeylist,t1word, tab1dbnamekey):
    slist=[]
    index=0
    print('start',tab1dbnamekey)
    for t2word in tab2dbnamevaluelist:
        #print(index)
        if difflib.SequenceMatcher(None, t1word, t2word).ratio() >.9 :
            #print ("score for: " + my_str + " vs. " + word + " = " + str(difflib.SequenceMatcher(None, my_str, word).ratio()))
            #print(index ,"-->",tab2dbnamekeylist[index],tab2dbnamevaluelist[index])
            slist.append(tab2dbnamekeylist[index])
        index=index+1 
    print('end',tab1dbnamekey)       
    return slist

def gettrantableNames(fname,sheetname,trange,keyrange):
    datadict={}
    try:
        ws = xw.Book(fname).sheets[sheetname]
        v1 = ws.range(keyrange).value
        v2 =  ws.range(trange).value 
        
        previous=int(v1[0])
        i=0
        concatstr =""
        for row in v2:
            cval=int(v1[i])             
            #print (cval)
            if cval != previous:
                concatstr =row[0]
                datadict[cval]=concatstr
                
            else:
                concatstr=concatstr+row[0]
                datadict[cval]=concatstr
            i=i+1
            previous=cval
    except Exception:
        traceback.print_exc()
     
    return datadict

def processkey(tab1dbnamekey):
    return tab1dbnamekey


def handleFunction(tab2dbnamevaluelist,tab2dbnamekeylist,t1word, tab1dbnamekey):
    return processdbdiff(tab2dbnamevaluelist,tab2dbnamekeylist,t1word, tab1dbnamekey), processkey(tab1dbnamekey)


def parallel_func(tab1dbnamekeylist,tab1dbnamevaluelist,tab2dbnamekeylist,tab2dbnamevaluelist):
    mdictionary= {}
    with ThreadPoolExecutor(max_workers=100) as executor:
        processes = []
        index=0
        for t1word in tab1dbnamevaluelist :
            processes.append(executor.submit(handleFunction, tab2dbnamevaluelist,tab2dbnamekeylist,t1word, tab1dbnamekeylist[index]))
            index=index+1
            if index > 1000 :
                break
        results = concurrent.futures.as_completed(processes)
        # Print list of tuples, one tuple per line with values separated by commas.
        #print('\n'.join(str(x.result()[0]) + ', ' + str(x.result()[1]) for x in results))
        for x in results :
            mdictionary[str(x.result()[1])]=x.result()[0]
    return  mdictionary
       
start = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

# printing current time in india
print("The current time in india is :", start)
rttab1=gettrantableNames('input_full.xlsx','Tab1','B2:C48000','A2:A48000')
rttab2=gettrantableNames('input_full.xlsx','Tab2','B2:C48000','A2:A48000')
tab2dbnamevaluelist = list(rttab2.values())
tab2dbnamekeylist = list(rttab2.keys())
tab1dbnamevaluelist = list(rttab1.values())
tab1dbnamekeylist = list(rttab1.keys())
print(len(tab2dbnamevaluelist))
print(len(tab2dbnamekeylist))
print(len(tab1dbnamevaluelist))
print(len(tab1dbnamekeylist))
ddd=parallel_func(tab1dbnamekeylist,tab1dbnamevaluelist,tab2dbnamekeylist,tab2dbnamevaluelist)
writeTojson('ddd.json',ddd)

end = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

# printing current time in india
print("The current time in india is :", end)
print(end-start)