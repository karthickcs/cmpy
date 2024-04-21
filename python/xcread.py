#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import xlwings as xw
import json
from lxml import etree,objectify
totalerr=""
import sys 
from operator import length_hint 
import dictdiffer 


# In[2]:


def getfdata(text,datadictval,tname, kkey):
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
            
            for x in r:        
                datadictval[tname+path]=x.text
    except:
        totalerr=kkey 
     
    return datadictval


# In[3]:


def readexceldata(fname,sheetname,trange,keyrange):
    datadictnew={}
    try:
        ws = xw.Book(fname).sheets[sheetname]
        v1 = ws.range(keyrange).value
        v2 =  ws.range(trange).value
        
        datadicttemp={}
        i=0
        for row in v2:
            try:
            	cval=int(v1[i])
            except:
                cval=0

            datadicttemp={}
            #print (cval)
            if cval in datadictnew:
                datadictnew[cval]=getfdata(row[1],datadictnew[cval],row[0],cval)
            else:
                datadictnew[cval]=getfdata(row[1],datadicttemp,row[0],cval)
            i=i+1
    except Exception as e: print(e)
     
    return datadictnew


# In[4]:


def writeTojson(fname, data):
    try:        
        with open(fname, 'w') as convert_file: 
            convert_file.write(json.dumps(data))
    except:
        print(fname ,'failed')
     
    return 


# In[5]:


def generateDiff(tab1, tab2):
    try:        
        keysListtab1 = list(tab1.keys())
        keysListtab2 = list(tab2.keys())
        Masterlist=[]
        for kvalmaster in keysListtab1:
            sublist=[]
            compscore=sys.maxsize
            matchkey=0;
            master=tab1[kvalmaster ]
            for kval in keysListtab2:
                follower=tab2[kval]
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength+ len(t[2])
                #print(list(dictdiffer.diff(master, follower)))
                #print(kval,difflistlength)
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                
            #print(kvalmaster ,matchkey, compscore)
            
            sublist.append(list(dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            Masterlist.append(sublist)
             
        df = pd.DataFrame(Masterlist)
    except:
        print(fname ,'failed')
     
    return df


# In[6]:


tab1=readexceldata('C:\\Users\\cskar\\Desktop\\python\\datatotest.xlsx','data1','C2:D40000','A2:AD40000')
tab2=readexceldata('C:\\Users\\cskar\\Desktop\\python\\datatotest.xlsx','data2','C2:DD40000','A2:AD40000')
#writeTojson('tab1.json',tab1)
#writeTojson('tab2.json',tab2)
df =generateDiff(tab1,tab2)


# In[7]:


df 


# In[8]:


df.to_csv('Finaldiff.csv')
print('done') 
 


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




