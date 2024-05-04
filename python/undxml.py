
import traceback
import pandas as pd
import xlwings as xw
import json
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

 

def getxmldata(fname,datadict):
    try:
        root = etree.parse(fname)
        i=0 
        #root = etree.fromstring(text).getroottree()
        ns = {'df': 'http://www.w3.org/TR/html4/', 'types': 'http://www.w3schools.com/furniture'}
        for e in root.iter():
          
            path = root.getelementpath(e)
            root_path =  '/' + root.getroot().tag
            if path == '.':
                path = root_path
            else:
                path = root_path + '/' + path
            for ns_key in ns:
                path = path.replace('{' + ns[ns_key] + '}', ns_key + ':')
             
            r = root.xpath(path, namespaces=ns)
            
            for x in r:
                if path.find("COLUMN[2]") > -1: 
                    i=i+1
                    datadictsubtable={}
                    datadictsubtable=getfdata(x.text,datadictsubtable,i)
                    if 'tablename' in datadictsubtable:
                        datadict[datadictsubtable["tablename"]]=datadictsubtable
                    else :
                        datadict["Failedtoprocess"]=x.text    
    except Exception:
        traceback.print_exc()
     
    return datadict
def readexceldata(fname,sheetname,trange):
    totalerr=""
    try:
        ws = xw.Book(fname).sheets[sheetname]
     
        v2 =  ws.range(trange).value
        datadict={}
        datadicttemp={}
        cvalold=-1
        i=0
        rval=0
        for row in v2:
            cval=row[0]
            if(len(cval)> 100):
                continue
            #print (cval)
                       
            #print (cval)    
            datadicttemp={}
             
            datadict[cval]=getfdata(row[1],datadicttemp,rval)
            i=i+1
            rval=rval+1
    except Exception:
        totalerr=totalerr+str(i)+""
    print(totalerr)
    return datadict

def getfdata(text,datadict,row):
    mytemp={}
    try:
        root = etree.fromstring(text).getroottree()
        ns = {'df': 'http://www.w3.org/TR/html4/', 'types': 'http://www.w3schools.com/furniture'}
        for e in root.iter():
          
            path = root.getelementpath(e)
            root_path =  '/' + root.getroot().tag
            if path == '.':
                path = root_path
            else:
                path = root_path + '/' + path
            for ns_key in ns:
                path = path.replace('{' + ns[ns_key] + '}', ns_key + ':')
             
            r = root.xpath(path, namespaces=ns)
            path=path.replace("/row/","")
            path=path[0:path.find("[")]
            for x in r:
                if path.find("c3") > -1 and   x.text.isnumeric(): 
                    att=x.attrib.keys() 
                    if len(att) ==0 :
                        datadict[path]=x.text
                    else:
                        aname=x.attrib.keys()[0]
                        if aname.find("id") > -1:
                            datadict["tablename"]=x.attrib[aname]   
                        else:
                            #datadict[path+"_"+str(aname)+"_"+x.attrib[aname]]=x.text
                            datadict["C"+x.text] =mytemp["Main"+x.attrib[aname]]
                if path.find("c1") > -1 : 
                    att=x.attrib.keys() 
                    if len(att) ==0 :
                        mytemp[path]=x.text
                    else:
                        aname=x.attrib.keys()[0]
                        if aname.find("id") > -1:
                            mytemp["tablename"]=x.attrib[aname]   
                        else:
                            #datadict[path+"_"+str(aname)+"_"+x.attrib[aname]]=x.text
                            mytemp["Main"+x.attrib[aname]] =x.text           
                               
                            
    except Exception:
        traceback.print_exc()
     
    return datadict
 

def writeTojson(fname, data):
    try:        
        with open(fname, 'w') as convert_file: 
            convert_file.write(json.dumps(data))
    except Exception:
        traceback.print_exc()
     
    return 


# In[6]:



current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
datadictt={}
# printing current time in india
print("The current time in india is :", current_time)
datadictt=readexceldata("ss.xlsx",'Tabelle1','A2:B'+str(5300))
print("done")
#datadictt=getxmldata("ss.xml",datadictt)
writeTojson('sam.json',datadictt)
 
 
 