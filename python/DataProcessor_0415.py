import cx_Oracle
import concurrent.futures
import pandas as pd
import xlwings as xw
import json
import difflib 
import threading
import time
from lxml import etree,objectify
import os
import traceback
import sys 
import html
from operator import length_hint 
import dictdiffer 
from collections import OrderedDict
import numpy as np
from collections import defaultdict
# for now()
import datetime
 
# for timezone()
import pytz
from threading import Thread

# In[2]:

def insertProws(taskid,differeceval,maintranid,comptranid,compscore,tid,sequence):
    cursor = None
    con = None
    
    try:
        json_mylist = str(differeceval).replace("'","\"")
        con = cx_Oracle.connect('myuser/password@localhost:1521/XEPDB1')
        cursor = con.cursor()
        statement="INSERT INTO  DIFF_TABLE (TASKID, INSERTTS, MAINTRANID, NEWTRANID, SEQUENCE, DIFFERENCE, COUNTVAL, THREADID) VALUES ('"+taskid+"', sysdate, '"+str(maintranid)+"', '"+str(comptranid)+"', '"+str(sequence)+"', '"+str(json_mylist)+"', '"+str(compscore)+"', '"+tid+"')"
        cursor.execute(statement)    		
        #print (statement)
        
        con.commit()
        
    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
            
def readexceldata(fname,sheetname,trange,keyrange):
    totalerr=""
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
            #print (cval)
                       
            #print (cval)    
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
            cvalold=cval
            
            if cval in datadict:
                datadict[cval]=getfdata(row[1],datadict[cval],row[0],cval,rval)
            else:
                datadict[cval]=getfdata(row[1],datadicttemp,row[0],cval,rval)
            i=i+1
            rval=rval+1
    except Exception:
        totalerr=totalerr+str(i)+""
    print(totalerr)
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
    except Exception:
        x=5
     
    return datadict

def getCountitem(tabdata):
    datadict={}
    try:
        keysListtab1 = list(tabdata.keys())
        for row in keysListtab1:
            #print(row)
            #print(len(tabdata[row]))
            cval=int(row)
            datadict[cval]=len(tabdata[row])  
            #datadict[int(row)]=len(tabdata[row])
            #break
    except Exception:
        traceback.print_exc()
     
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
# In[5]:


def writeTojson(fname, data):
    try:        
        with open(fname, 'w') as convert_file: 
            convert_file.write(json.dumps(data))
    except:
        print(fname ,'failed')
     
    return 
 
def dproces(name,start,end, tab1, tab2, counttab1 , counttab2 , tnameConcattab1 , tnameConcattab2, tnamereversemap , taskid):
    try:
               
        keysListtab1 = list(tab1.keys())
        #print(name,":",start,":",end,";",len(keysListtab1[start:end]),":**") 
        #print(keysListtab1[sizeval:sizeval+10])
        i=0
        Masterlist=[]
        counter = start
        for kvalmaster in keysListtab1[start:end] :
            #if str(kvalmaster) != '1010074298' :
                #continue
            i=i+1
            if i>10000 :
                break
            sublist=[]
            compscore=sys.maxsize
            matchkey=0;
            master=tab1[kvalmaster ]
            #filtered_dict = dict(filter(lambda item: counttab1[kvalmaster]-10 <= item[1] <= counttab1[kvalmaster]+10, counttab2.items()))
            #keysListtab2 = list(filtered_dict.keys())
            keysListtab2=tnamereversemap[tnameConcattab1[kvalmaster]]
            #print(name,":",counter,":",kvalmaster,";",len(filtered_dict),":**",len(master))
            counter=counter+1
            for kval in keysListtab2:
                #if( tnameConcattab1[kvalmaster] != tnameConcattab2[kval]) :
                    #continue
                    
                
                #if str(kval) != '1010074298' :
                    #continue
                follower=tab2[kval]
                #print(kvalmaster)
                #print(kval)
                difflist = json(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength+ len(t[2])
                #print(list(dictdiffer.diff(master, follower)))
                #print(kvalmaster,kval,difflistlength)
                #print(difflistlength)
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                if difflistlength<= 0 :
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
            insertProws(taskid,list(dictdiffer.diff(master, tab2[matchkey])),kvalmaster,matchkey,compscore,name ,i)
            
            
            
        df = pd.DataFrame(Masterlist)
        df.to_csv('Finaldiff.csv', mode='a', header=False)
        
    except Exception as error:
        print("An exception occurred:", error)


#######################################################################################
#########################################################################################
#################################################################################

def mainProgram(fname,rowcount,taskid):
    try:
        start = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        print("Start --->:", start)
        tranDataTab1=readexceldata(fname,'Tab1','B2:C'+str(rowcount),'A2:A'+str(rowcount))
        tranDataTab2=readexceldata(fname,'Tab2','B2:C'+str(rowcount),'A2:A'+str(rowcount))
        #writeTojson('tab1.json',tranDataTab1)
        #writeTojson('tab2.json',tranDataTab2)
        tnameConcattab1=gettrantableNames(fname,'Tab1','B2:C'+str(rowcount),'A2:A'+str(rowcount))
        tnameConcattab2=gettrantableNames(fname,'Tab2','B2:C'+str(rowcount),'A2:A'+str(rowcount))
        #writeTojson('rttab2.json',tnameConcattab2) 
        #writeTojson('rttab2.json',tnameConcattab2)
        res = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            res[val].append(key)
        #writeTojson('rttabxxx.json',dict(res))     
        # printing result 
        tnamereversemap=dict(res)
        #kk = list(tnamereversemap.keys())
        #countdict ={}
        #for x in kk :
            #countdict[x]=len(tnamereversemap[x])
        #writeTojson('helllll.json',countdict)     
        tab2dbnamevaluelist = list(tnameConcattab2.values())
        tab2dbnamekeylist = list(tnameConcattab2.keys())
        tab1dbnamevaluelist = list(tnameConcattab1.values())
        tab1dbnamekeylist = list(tnameConcattab1.keys())
        counttab1=getCountitem(tranDataTab1)
        counttab2=getCountitem(tranDataTab2)
        #writeTojson('ctab1.json',counttab1)
        #writeTojson('ctab2.json',counttab2)
        Masterlist=[]
        df = pd.DataFrame(Masterlist)
        df.to_csv('Finaldiff.csv', header='column_names')

        threadrunner =[]
        rval=0
        triger=10
        for i in range(1400):
            t = threading.Thread(target=dproces,args=("T-"+str(i),rval,rval+triger, tranDataTab1, tranDataTab2, counttab1 , counttab2, tnameConcattab1 , tnameConcattab2, tnamereversemap,taskid))
            t.start()
            threadrunner.append(t)
            rval=rval+triger
            
        for threadm in threadrunner :
            threadm.join()    
        print(len(tranDataTab1)) 
        print ("Exiting Main Thread") 
        end = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        print("end :", end)
        print("Total time ===== :",end-start)
        print('done') 
    except Exception as error:
        print("An exception occurred:", error)


