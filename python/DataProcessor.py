#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


def readexceldata(fname,sheetname,trange,keyrange):
    try:
        ws = xw.Book(fname).sheets[sheetname]
        v1 = ws.range(keyrange).value
        v2 =  ws.range(trange).value
        datadict={}
        datadicttemp={}
        cvalold=-1
        i=0
        rval=0
        for row in v2:
            cval=int(v1[i])
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
            cvalold=cval
            #print (cval)
            if cval in datadict:
                datadict[cval]=getfdata(row[1],datadict[cval],row[0],cval,rval)
            else:
                datadict[cval]=getfdata(row[1],datadicttemp,row[0],cval,rval)
            i=i+1
            rval=rval+1
    except:
        print(fname,sheetname ,'failed555')
     
    return datadict


# In[3]:


def getfdata(text,datadict,tname, kkey, rowid):
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
                datadict[tname+"_"+str(rowid)+"_"+path]=x.text
    except:
        totalerr=kkey 
     
    return datadict


# In[4]:


def gettrantablecount(fname,sheetname,trange,keyrange):
    datadict={}
    try:
        ws = xw.Book(fname).sheets[sheetname]
        v1 = ws.range(keyrange).value
         
        
        previous=int(v1[0])
        i=0
        counter =0
        for row in v1:
            cval=int(row)             
            #print (cval)
            if cval != previous:
                counter =1
                datadict[cval]=counter
                
            else:
                counter=counter+1
                datadict[cval]=counter
            i=i+1
            previous=cval
    except Exception:
        traceback.print_exc()
     
    return datadict
    


# In[5]:


def writeTojson(fname, data):
    try:        
        with open(fname, 'w') as convert_file: 
            convert_file.write(json.dumps(data))
    except:
        print(fname ,'failed')
     
    return 


# In[6]:


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
                #print(kvalmaster,kval,difflistlength)
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                if difflistlength<5 :
                    break

            #print(kvalmaster ,matchkey, compscore)
            
            sublist.append(list(dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            Masterlist.append(sublist)
             
        df = pd.DataFrame(Masterlist)
    except:
        print( 'failed')
     
    return df


# In[7]:
class myThread (threading.Thread):
   def __init__(self, name,sizeval, tab1, tab2, ctab1sorted , ctab2sorted):
      threading.Thread.__init__(self)
      self.name = name
      self.sizeval= sizeval
      self.tab1 =tab1
      self.tab2 =tab2
      self.ctab1sorted =ctab1sorted
      self.ctab2sorted =ctab2sorted

   def run(self):
        try:        
            keysListtab1 = list(self.tab1.keys())
            #print(keysListtab1[self.sizeval:self.sizeval+10])
            i=0
            Masterlist=[]
            for kvalmaster in keysListtab1[self.sizeval:self.sizeval+99]:
                i=i+1
                if i>10000 :
                    break
                sublist=[]
                compscore=sys.maxsize
                matchkey=0;
                master=self.tab1[kvalmaster ]
                filtered_dict = dict(filter(lambda item: self.ctab1sorted[kvalmaster]-1 <= item[1] <= self.ctab1sorted[kvalmaster]+1, self.ctab2sorted.items()))
                keysListtab2 = list(filtered_dict.keys())
                print(self.name,":",i,":",kvalmaster,";",len(filtered_dict),":**",len(master))
                for kval in keysListtab2:
                    follower=self.tab2[kval]
                    #print(kvalmaster)
                    #print(kval)
                    difflist = list(dictdiffer.diff(master, follower))
                    difflistlength=0
                    for t in difflist:
                        difflistlength=difflistlength+ len(t[2])
                    #print(list(dictdiffer.diff(master, follower)))
                    #print(kvalmaster,kval,difflistlength)
                    #print(difflistlength)
                    if difflistlength<compscore :
                        compscore=difflistlength
                        matchkey= kval
                    if difflistlength<= 3 :
                        break
                        #print(difflistlength)
                        #print(master)
                        #print(follower)
                #print(kvalmaster ,matchkey, compscore)
                
                sublist.append(list(dictdiffer.diff(master, tab2[matchkey])))
                sublist.append(kvalmaster )
                sublist.append(matchkey)
                sublist.append(compscore)
                Masterlist.append(sublist)
                
                
                
            df = pd.DataFrame(Masterlist)
            df.to_csv('Finaldiff'+self.name+'.csv')
        except Exception as error:
            print("An exception occurred:", error)
     
     
        


def generateDiffnewlogic(tab1, tab2, ctab1sorted , ctab2sorted):
     
    try:        
        keysListtab1 = list(tab1.keys())
        i=0
        Masterlist=[]
        for kvalmaster in keysListtab1:
            i=i+1
            
            sublist=[]
            compscore=sys.maxsize
            matchkey=0;
            master=tab1[kvalmaster ]
            filtered_dict = dict(filter(lambda item: ctab1sorted[kvalmaster]-1 <= item[1] <= ctab1sorted[kvalmaster]+1, ctab2sorted.items()))
            keysListtab2 = list(filtered_dict.keys())
            print(":",i,":",kvalmaster,";",len(filtered_dict),":**",len(master))
            for kval in keysListtab2:
                follower=tab2[kval]
                #print(kvalmaster)
                #print(kval)
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength+ len(t[2])
                #print(list(dictdiffer.diff(master, follower)))
                #print(kvalmaster,kval,difflistlength)
                #print(difflistlength)
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                if difflistlength<= 3 :
                    break
                    #print(difflistlength)
                    #print(master)
                    #print(follower)
            #print(kvalmaster ,matchkey, compscore)
            
            sublist.append(list(dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            Masterlist.append(sublist)
            
            
             
        df = pd.DataFrame(Masterlist)
    except:
        print( 'failed')
     
    return df


# In[8]:


current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

# printing current time in india
print("The current time in india is :", current_time)
tab1=readexceldata('input.xlsx','Tab1','B2:C480','A2:A480')
tab2=readexceldata('input.xlsx','Tab2','B2:C480','A2:A480')
writeTojson('tab1.json',tab1)
writeTojson('tab2.json',tab2)
#df =generateDiff(tab1,tab2)
#df.to_csv('Finaldiff.csv')


# In[9]:


#print(list(tab2.keys())[0])
#print(tab2[list(tab2.keys())[0]])
 


# In[10]:


ctab1=gettrantablecount('input.xlsx','tab1','B2:C480','A2:A480')
ctab2=gettrantablecount('input.xlsx','tab2','B2:C480','A2:A480')


# In[11]:


keys = list(ctab1.keys())
values = list(ctab1.values())
sorted_value_index = np.argsort(values)
descending_indices = sorted_value_index[::-1] 
ctab1sorted = {keys[i]: values[i] for i in descending_indices}
keys1 = list(ctab2.keys())
values1 = list(ctab2.values())
sorted_value_index1 = np.argsort(values1)
descending_indices1 = sorted_value_index1[::-1] 
ctab2sorted = {keys1[i]: values1[i] for i in descending_indices1}
writeTojson('ctab1.json',ctab1sorted)
writeTojson('ctab2.json',ctab2sorted) 


# In[ ]:


#df =generateDiffnewlogic(tab1, tab2, ctab1sorted , ctab2sorted)
thread1 = myThread("T-1",1, tab1, tab2, ctab1sorted , ctab2sorted)
thread2 = myThread("T-2",100,tab1, tab2, ctab1sorted , ctab2sorted)
thread3 = myThread("T-3",300,tab1, tab2, ctab1sorted , ctab2sorted)
thread4 = myThread("T-4",400,tab1, tab2, ctab1sorted , ctab2sorted)
thread5 = myThread("T-5",500,tab1, tab2, ctab1sorted , ctab2sorted)
thread6 = myThread("T-6",600,tab1, tab2, ctab1sorted , ctab2sorted)
thread7 = myThread("T-7",700,tab1, tab2, ctab1sorted , ctab2sorted)
thread8 = myThread("T-8",800,tab1, tab2, ctab1sorted , ctab2sorted)
thread9 = myThread("T-9",900,tab1, tab2, ctab1sorted , ctab2sorted)
thread10 = myThread("T-10",1000,tab1, tab2, ctab1sorted , ctab2sorted)

# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()
thread9.start()
thread10.start()
thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()
thread6.join()
thread7.join()
thread8.join()
thread9.join()
thread10.join()
print ("Exiting Main Thread") 

print('done') 


# In[ ]:


#filtered_dict = dict(filter(lambda item: ctab1sorted[keysListtab1[1]]-10 <= item[1] <= ctab1sorted[keysListtab1[1]]+10, ctab2sorted.items()))
#keysListtab1 = list(ctab1sorted.keys())
#print(ctab1sorted[keysListtab1[1]])
#print(keysListtab1[1]) 
#print(filtered_dict)


# In[ ]:


current_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
 
# printing current time in india
print("The current time in india is :", current_time)


# In[ ]:





# In[ ]:




