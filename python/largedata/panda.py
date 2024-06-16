import pandas as pd
from datetime import datetime
import cx_Oracle
# importing module

import psycopg2
import pandas as pd
from datetime import timedelta
import json
import os
from lxml import etree,objectify
import sys 
from operator import length_hint 
import dictdiffer 
import time
#from DataProcessor_0415pg import mainProgram
import psycopg2
import concurrent.futures
import pandas as pd
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
from datetime import datetime

import csv 
# for timezone()
import pytz
from threading import Thread
import psycopg2
import concurrent.futures
import pandas as pd
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

import csv 
# for timezone()
import pytz
from threading import Thread

def insertProws(taskid,runid,differeceval,maintranid,comptranid,compscore,tid,sequence):
    cursor = None
    con = None
    
    try:
        json_mylist = str(differeceval).replace("'","\"")
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="INSERT INTO  DIFF_TABLE (TASKID,runid, INSERTTS, MAINTRANID, NEWTRANID, SEQUENCE, DIFFERENCE, COUNTVAL, THREADID) VALUES ('"+str(taskid)+"','"+str(runid)+"', Current_timestamp, '"+str(maintranid)+"', '"+str(comptranid)+"', '"+str(sequence)+"', '"+str(json_mylist)+"', '"+str(compscore)+"', '"+str(tid)+"')"
        cursor.execute(statement)    		
        #print (statement)
        
        con.commit()
        
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
            
def reaxmldata(scndetails):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    try:       
        for index, row in scndetails.iterrows():
            # print(row["TNAME"], row["INFORMATION"])
             
            
            cval=row["SCNNUM"]
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
                cvalold=cval            
            if cval in datadict:
                datadict[cval]=getfdata(row["INFORMATION"],datadict[cval],row["TNAME"],cval,i,row["ROWIDVAL"])
            else:
                datadict[cval]=getfdata(row["INFORMATION"],datadicttemp,row["TNAME"],cval,i,row["ROWIDVAL"])
            i=i+1
            rval=rval+1
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict

def getfdata(text,datadict,tname, kkey, rowid ,recid):

    try:
        if text.find("</row>") < 0 :
            text = text[0:text.rfind(">\n")+1]+"</row>"
            
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
            path=path.replace("row",recid)
            for x in r:        
                datadict[tname+":"+str(rowid)+":"+path]=x.text
    except Exception:
        ii=1
     
    return datadict

def gettrantableNames(scndetails):
    totalerr=""
    datadict={}     
    i=0
    rval=0
    try:
        previous=0
        i=0
        concatstr =""
        for index, row in scndetails.iterrows():
            cval=row["SCNNUM"]           
        
            if cval != previous:
                concatstr =row["TNAME"]
                datadict[cval]=concatstr
                
            else:
                concatstr=concatstr+row["TNAME"]
                datadict[cval]=concatstr
            i=i+1
            previous=cval                
    except Exception:
        traceback.print_exc()
    return datadict

def innerthread(tid,start,end,tab1,tab2,tnameConcattab1,tnameConcattab2,tnamereversemap,taskid,runid):
    try:    
        Masterlist=[]
        i=0 
        ptimead(tid,'start')
        keysListtab1 = list(tab1.keys())
        for kvalmaster in keysListtab1[start:end] :
            i=i+1             
            sublist=[]
            compscore=sys.maxsize
            matchkey=0;
            master=tab1[kvalmaster ]
            keysListtab2=tnamereversemap[tnameConcattab1[kvalmaster]]
            for kval in keysListtab2:
                
                follower=tab2[kval]
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength + len(t[2])
                
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                if difflistlength<= 0 :
                    break
                                
            sublist.append(list( dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            sublist.append(len(keysListtab2))
            Masterlist.append(sublist)
            if compscore > 0:
                insertProws(taskid,runid,list(list(dictdiffer.diff(master, tab2[matchkey]))),kvalmaster,matchkey,compscore,tid ,i)
    except Exception:
        traceback.print_exc()
    df = pd.DataFrame(Masterlist)
    ptimead(tid,'end')
    df.to_csv('Finaldiff.csv', mode='a', header=False)
    ptimead(tid,'endfil')
    
    
def dproces(tid,scndetssys1,scndetssys2,taskid,runid):
    try:
           
        tab1=reaxmldata(scndetssys1)
        tab2=reaxmldata(scndetssys2)       
        keysListtab1 = list(tab1.keys()) 
        #keysListtab2 = list(tab2.keys()) 
        tnameConcattab1=gettrantableNames(scndetssys1)
        tnameConcattab2=gettrantableNames(scndetssys2)
       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)
        
        tnamereversemap=dict(dres)
        
        Masterlist=[]
        i=0 
        for kvalmaster in keysListtab1 :
            i=i+1             
            sublist=[]
            compscore=sys.maxsize
            matchkey=0;
            master=tab1[kvalmaster ]
            keysListtab2=tnamereversemap[tnameConcattab1[kvalmaster]]
            for kval in keysListtab2:
                
                follower=tab2[kval]
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength+ 1
                
                if difflistlength<compscore :
                    compscore=difflistlength
                    matchkey= kval
                if difflistlength<= 0 :
                    break
                              
            sublist.append(list( dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            sublist.append(len(keysListtab2))
            Masterlist.append(sublist)
            if compscore > 0:
                insertProws(taskid,runid,list(list(dictdiffer.diff(master, tab2[matchkey]))),kvalmaster,matchkey,compscore,tid ,i)
       
    except Exception:
        traceback.print_exc()
    df = pd.DataFrame(Masterlist)
    df.to_csv('Finaldiff.csv', mode='a', header=False)



def dprocesmulti(tid,scndetssys1,scndetssys2,taskid,runid):
    try:
        print ("start inner  Thread of "+tid) 
        ptime()
        tab1=reaxmldata(scndetssys1)
        tab2=reaxmldata(scndetssys2)       
        keysListtab1 = list(tab1.keys()) 
        tnameConcattab1=gettrantableNames(scndetssys1)
        tnameConcattab2=gettrantableNames(scndetssys2)       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)
        intriger=len(tab1)
        tnamereversemap=dict(dres)
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
        if inrr==0:
            inrr=1
        print ("start inner  Thread of "+tid) 
        for i in range(1):
            t = threading.Thread(target=innerthread,args=("T-"+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
           
            t.start()
            print ("start inner  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        print ("Exiting inner  Thread of "+tid) 
        
        
       
    except Exception:
        traceback.print_exc()
    


def ptimead(tid,ty):
    print(ty+'  time -->',datetime.now(pytz.timezone('Asia/Kolkata')),tid)
    
def ptime():
    print('  time -->',datetime.now(pytz.timezone('Asia/Kolkata')))    



####################################################################
os.system('cls')
start = datetime.now(pytz.timezone('Asia/Kolkata'))
print("Start --->:", start)
ptime() 
sys1df = pd.read_csv('sys1.csv')
sys1df['UPDATETS']=pd.to_datetime(sys1df['UPDATETS'].astype(str), format='%d-%b-%y %I.%M.%S.%f000 %p')
#rslt_sys1df = sys1df.loc[(sys1df['UPDATETS']> datetime.strptime('22-MAY-24 09.47.59.712000000 AM', '%d-%b-%y %I.%M.%S.%f000 %p')) & (sys1df['UPDATETS']< datetime.strptime('22-MAY-24 09.50.59.712000000 AM', '%d-%b-%y %I.%M.%S.%f000 %p')) ] 
rslt_sys1df = sys1df.loc[(sys1df['TNAME']=='UPDATING on F_BATCH') & (~ sys1df['INFORMATION'].str.contains("<c3>0</c3>"))] 
# rslt_sys1df.to_csv('outsys1.csv') 
resultsys1 = rslt_sys1df.groupby('ROWIDVAL').agg({'UPDATETS': [ 'min', 'max']})  
resultsys1.to_csv('resultsys1.csv')
ptime()
sys2df = pd.read_csv('sys2.csv')
sys2df['UPDATETS']=pd.to_datetime(sys2df['UPDATETS'].astype(str), format='%d-%b-%y %I.%M.%S.%f000 %p')
#rslt_sys2df = sys2df.loc[(sys2df['UPDATETS']> datetime.strptime('22-MAY-24 09.47.59.712000000 AM', '%d-%b-%y %I.%M.%S.%f000 %p')) & (sys2df['UPDATETS']< datetime.strptime('22-MAY-24 09.50.59.712000000 AM', '%d-%b-%y %I.%M.%S.%f000 %p')) ] 
rslt_sys2df = sys2df.loc[(sys2df['TNAME']=='UPDATING on F_BATCH') & (~ sys2df['INFORMATION'].str.contains("<c3>0</c3>"))] 
# rslt_sys2df.to_csv('out.csv') 
resultsys2 = rslt_sys2df.groupby('ROWIDVAL').agg({'UPDATETS': [ 'min', 'max']}) 
x=rslt_sys2df.groupby(["ROWIDVAL"], as_index=False).agg({'UPDATETS': [ 'min', 'max']}) 
resultsys2.to_csv('resultsys2.csv')
ptime()
sys1finaldf=pd.read_csv('resultsys1.csv')
sys2finaldf=pd.read_csv('resultsys2.csv')

sys2finaldf.columns = ['name', 'min', 'max']
# newdf[ 'min']=pd.to_datetime(newdf[ 'min'] )
# newdf[ 'max']=pd.to_datetime(newdf[ 'max'] )
print ("start Main Thread") 
taskid=18
runid=4
Masterlist=[] 
threadrunner =[]
df = pd.DataFrame(Masterlist)
df.to_csv('Finaldiff.csv', header='column_names')
    
for i in range(len(sys1finaldf)):
    if i >=2 :
        batch=sys1finaldf.iloc[i,0]
        mintime=sys1finaldf.iloc[i, 1]
        maxtime= sys1finaldf.iloc[i, 2] 
        scndetssys1 = sys1df.loc[(sys1df['UPDATETS']> datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S.%f')) & (sys1df['UPDATETS']< datetime.strptime(maxtime, '%Y-%m-%d %H:%M:%S.%f')) & (sys2df['TNAME']!='UPDATING on F_LOCKING')] 
        sys2batchdet = sys2finaldf.loc[(sys2finaldf['name']==batch) ]  
        batchsys2=sys2batchdet['name'].iloc[0]
        mintimesys2=sys2batchdet['min'].iloc[0]
        maxtimesys2= sys2batchdet['max'].iloc[0]
        scndetssys2 = sys2df.loc[(sys2df['UPDATETS']> datetime.strptime(mintimesys2, '%Y-%m-%d %H:%M:%S.%f')) & (sys2df['UPDATETS']< datetime.strptime(maxtimesys2, '%Y-%m-%d %H:%M:%S.%f'))  & (sys2df['TNAME']!='UPDATING on F_LOCKING')] 
        if batch!= "BNK/AC.STMT.UPDATE" :
             continue     
        print(batch,scndetssys1.shape[0],scndetssys2.shape[0])
        # if scndetssys1.shape[0] < 2000:
        #     t = threading.Thread(target=dproces,args=("T-"+str(i),scndetssys1,scndetssys2,taskid,runid))
        # else:
        #dprocesmulti("T-"+str(i),scndetssys1,scndetssys2,taskid,runid)
        t = threading.Thread(target=dprocesmulti,args=("T-"+str(i),scndetssys1,scndetssys2,taskid,runid))
        t.start()
        threadrunner.append(t)
        
             
for threadm in threadrunner :
    threadm.join()    
         
print ("Exiting Main Thread") 
end = datetime.now(pytz.timezone('Asia/Kolkata'))
print("end :", end)
print("Total time ===== :",end-start)
print('done')             

print('done')
 