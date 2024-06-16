import cx_Oracle
# importing module
import oracledb
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

# In[2]:

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
            
def readcsvdata(fname,row):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    try:
        with open(fname, mode ='r')as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                if i==0 :
                    i=i+1
                    continue
                if i>row :
                    break
                #print(lines)
                cval=lines[2]
                datadicttemp={}
                if(cval != cvalold) :
                    rval=1;
                    cvalold=cval            
                if cval in datadict:
                    datadict[cval]=getfdata(lines[3],datadict[cval],lines[0],cval,i)
                else:
                    datadict[cval]=getfdata(lines[3],datadicttemp,lines[0],cval,i)
                i=i+1
                rval=rval+1
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict

def getfdata(text,datadict,tname, kkey, rowid):

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
            
            for x in r:        
                datadict[tname+":"+str(rowid)+":"+path]=x.text
    except Exception:
        ii=1
     
    return datadict



def getkeybaseddict(tabdata):
    datadict={}
    try:
        keysListtab1 = list(tabdata.keys())
        for row in keysListtab1:
            #print(row)
            #print(len(tabdata[row]))
            arrstr=row.split("|")
            cval=arrstr[0]
            if cval in datadict:
                datadict[cval]= datadict[cval]+"~"+row
            else :
                 datadict[cval]= row
            #datadict[int(row)]=len(tabdata[row])
            #break
    except Exception:
        traceback.print_exc()
     
    return datadict

def getCountitem(tabdata):
    datadict={}
    try:
        keysListtab1 = list(tabdata.keys())
        for row in keysListtab1:
            #print(row)
            #print(len(tabdata[row]))
            cval=row
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
         
        
        previous=v1[0]
        i=0
        counter =0
        for row in v1:
            cval=row             
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
    

def gettrantableNames(fname,row):
    totalerr=""
    datadict={}     
    i=0
    rval=0
    try:
        previous=0
        i=0
        concatstr =""
        with open(fname, mode ='r')as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                if i==0 :
                    i=i+1
                    continue
                if i>row :
                    break
                #print(lines)
                cval=lines[2]            
                #print (cval)
                if cval != previous:
                    concatstr =lines[0]
                    datadict[cval]=concatstr
                    
                else:
                    concatstr=concatstr+lines[0]
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
            convert_file.write(json.dumps(data, indent=4, sort_keys=True, default=str))
            
    except Exception:
        traceback.print_exc()
     
    return 
 
def dproces(name,start,end, tab1, tab2 ,keybaseddict,tnameConcattab1 , tnameConcattab2, tnamereversemap, taskid , runid):
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
            #keysListtab2=list(tab2.keys())
            #print(name,":",counter,":",kvalmaster,";",len(filtered_dict),":**",len(master))
            try :
                # kmarr= kvalmaster.split("|")  
                # keybaseddictvalues=keybaseddict[kmarr[0]]
                # keybaseddictvaluesArr=keybaseddictvalues.split("~")
                keysListtab2=tnamereversemap[tnameConcattab1[kvalmaster]]
            except Exception:
                traceback.print_exc()    
            counter=counter+1
            ccounter=0
            for kval in keysListtab2:
                #if( tnameConcattab1[kvalmaster] != tnameConcattab2[kval]) :
                    #continue
                # if kval.split("|")[2]  not in  keysListtab2:
                #     continue
                #if str(kval) != '1010074298' :
                    #continue
                follower=tab2[kval]
                #print(kvalmaster)
                #print(kval)
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength+ 1
                    #print(t)
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
            
            sublist.append(list( dictdiffer.diff(master, tab2[matchkey])))
            sublist.append(kvalmaster )
            sublist.append(matchkey)
            sublist.append(compscore)
            sublist.append(len(keysListtab2))
            Masterlist.append(sublist)
            if compscore > 0:
                insertProws(taskid,runid,list(list(dictdiffer.diff(master, tab2[matchkey]))),kvalmaster,matchkey,compscore,name ,i)
            
            
            
        
        
    except Exception:
        traceback.print_exc()
    df = pd.DataFrame(Masterlist)
    df.to_csv('Finaldiff.csv', mode='a', header=False)

#######################################################################################
#########################################################################################
#################################################################################

def mainProgram(fname1,fname2,rowcount,taskid,runid):
    try:
        
        start = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("Start --->:", start)
        tranDataTab1=readcsvdata(fname1,rowcount)
        tranDataTab2=readcsvdata(fname2,rowcount)
      
        start = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("Start --->:", start)
         
        Masterlist=[]
        tnameConcattab1=gettrantableNames(fname1,rowcount)
        tnameConcattab2=gettrantableNames(fname2,rowcount)
       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            res[val].append(key)
        
        tnamereversemap=dict(dres)
          
        tab2dbnamevaluelist = list(tnameConcattab2.values())
        tab2dbnamekeylist = list(tnameConcattab2.keys())
        tab1dbnamevaluelist = list(tnameConcattab1.values())
        tab1dbnamekeylist = list(tnameConcattab1.keys())
        
        
        scndata=readcsvdatascn("sys1.csv",290000) 
        fbatdata=readcsvdatafbatch("fbatch.csv",15000)
        keysfbat = list(fbatdata.keys())
       
        for row in keysfbat:
            time_change = timedelta(minutes=0) 
            min=fbatdata[row]['minval']-time_change
            max=fbatdata[row]['maxval']+time_change
            res = {key : val for key, val in scndata.items()
                        if val >= min  and val <= max}
            key_string = ','.join(res.keys())
            
            if res :
                fbatdata[row]['scn']=key_string
                fbatdata[row]['count']=len(res.keys())
                
                
        scndata2=readcsvdatascn("sys2.csv",290000) 
        fbatdata2=readcsvdatafbatch("fbatch.csv",15000)
        keysfbat2 = list(fbatdata2.keys())
        
        for row in keysfbat2:
            time_change = timedelta(minutes=0) 
            min2=fbatdata2[row]['minval']-time_change
            max2=fbatdata2[row]['maxval']+time_change
            res2 = {key : val for key, val in scndata.items()
                        if val >= min2  and val <= max2}
            key_string2 = ','.join(res2.keys())
            
            if res :
                fbatdata2[row]['scn']=key_string2
                fbatdata2[row]['count']=len(res2.keys())        

        # writeTojson('fbatruncomp.json',fbatdata)
        # writeTojson('scndata.json',scndata)
        # writeTojson('scnkkkkk.json',{ "data" : finalstr })
        # print('completed')  

        
        df = pd.DataFrame(Masterlist)
        df.to_csv('Finaldiff.csv', header='column_names')
        keybaseddict={}
        threadrunner =[]
        rval=0
        triger=150
        print ("start Main Thread") 
        for i in range(2000):
            t = threading.Thread(target=dproces,args=("T-"+str(i),rval,rval+triger, tranDataTab1, tranDataTab2,keybaseddict, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
            t.start()
            threadrunner.append(t)
            rval=rval+triger
            
        for threadm in threadrunner :
            threadm.join()    
        print(len(tranDataTab1)) 
        print ("Exiting Main Thread") 
        end = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("end :", end)
        print("Total time ===== :",end-start)
        print('done') 
    except Exception as error:
        print("An exception occurred: main", error)

#mainProgram('Inputmaxsys1.csv','Inputmaxsys2.csv',48000,333,1)


def readDbData(sysdata,row):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    try:
        
        for lines in sysdata:
             
            if i>row :
                break
            #print(lines)
            cval=lines[2]
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
                cvalold=cval            
            if cval in datadict:
                datadict[cval]=getfdata(lines[3],datadict[cval],lines[0],cval,rval)
            else:
                datadict[cval]=getfdata(lines[3],datadicttemp,lines[0],cval,rval)
            i=i+1
            rval=rval+1
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict


def gettrantableNamesSysTable(sysdata,row):
    totalerr=""
    datadict={}     
    i=0
    rval=0
    try:
        previous=0
        i=0
        concatstr =""
        
        for lines in sysdata:
            
            if i>row :
                break
            #print(lines)
            cval=lines[2]             
            #print (cval)
            if cval != previous:
                concatstr =lines[0]
                datadict[cval]=concatstr
                
            else:
                concatstr=concatstr+lines[0]
                datadict[cval]=concatstr
            i=i+1
            previous=cval                
    except Exception:
        traceback.print_exc()
    return datadict


def mainProgramDbApproach(sys1,sys2,rowcount,taskid,runid):
    try:
        
        start = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("Start --->:", start)
        tranDataTab1=readDbData(sys1,rowcount)
        tranDataTab2=readDbData(sys2,rowcount)
        #writeTojson('tab1.json',tranDataTab1)
        #writeTojson('tab2.json',tranDataTab2)
        start = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("Start --->:", start)
        keybaseddict=getkeybaseddict(tranDataTab2)  
        Masterlist=[]
        tnameConcattab1=gettrantableNamesSysTable(sys1,rowcount)
        tnameConcattab2=gettrantableNamesSysTable(sys2,rowcount)
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
        
        df = pd.DataFrame(Masterlist)
        df.to_csv('Finaldiff.csv', header='column_names')

        threadrunner =[]
        rval=0
        triger=10
        print ("start Main Thread") 
        for i in range(50):
            t = threading.Thread(target=dproces,args=("T-"+str(i),rval,rval+triger, tranDataTab1, tranDataTab2,keybaseddict, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
            t.start()
            threadrunner.append(t)
            rval=rval+triger
            
        for threadm in threadrunner :
            threadm.join()    
        print(len(tranDataTab1)) 
        print ("Exiting Main Thread") 
        end = datetime.now(pytz.timezone('Asia/Kolkata'))
        print("end :", end)
        print("Total time ===== :",end-start)
        print('done') 
    except Exception as error:
        print("An exception occurred: main", error)

 

 
def tableListener():
    cursor = None
    con = None
    
    try:
         
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where status='CREATED'  and (select count(status) from DP_LISTEN_TABLE where status='INPROGRESS' ) = 0 order by insertts"
        cursor.execute(statement)    		
        results = cursor.fetchone()
        if results == None :
            return
        upstatement ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\''+str(results[1]) +'\' and runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement)
        con.commit()
        print('success  ',results)
        if results[24]  == "DB" :
            sys1=conecttoOracle(results[11],results[12],results[10],results[14],results[15],results[16])
            sys2=conecttoOracle(results[18],results[19],results[17],results[21],results[22],results[23])
            mainProgramDbApproach(sys1,sys2,results[6],results[1],results[7])
        else :
            mainProgram(results[2],results[8],results[6],results[1],results[7])
                
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(results[1]) +'\' and  runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

 

oracledb.init_oracle_client(lib_dir=r"C:\Users\cskar\Downloads\instantclient-basic-windows.x64-19.22.0.0.0dbru\instantclient_19_22")

def conecttoOracle(user,password,jdbcurl,tname,startts,endts):
    cursor = None
    con = None
    
    try:
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        
        cursor = con.cursor()
        statement="select * from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') order by scnnum,updatets,tname"
        cursor.execute(statement)    		
        #results = cursor.fetchone()
        remaining_rows = cursor.fetchall()
        if remaining_rows == None :
            return
        return remaining_rows
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

 

# a loop to run the check every 2 seconds- as to lessen cpu usage
def sleepLoop():
    while 0 == 0:
        tableListener()
        print("Waiting**********----->")
        time.sleep(10.0)
os.system('cls')
print('hi')
#sleepLoop()
def readcsvdatafbatch(fname,row):
    totalerr=""
    datadict={}
    Masterlist=[]
    cvalold=-1
    i=0
    rval=0
    try:
        with open(fname, mode ='r')as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                if i==0 :
                    i=i+1
                    continue
                i=i+1
                if i>row :
                    break
                #print(lines)
                cval=lines[1]
                 
                if lines[3].find("<c3>0</c3>") >0 :
                    continue
                date_time = datetime.strptime(lines[4], '%d-%b-%y %I.%M.%S.%f000 %p')
                if cval in datadict:
                    # print(date_time)
                    # print(datadict[cval]['minval'])
                    # print(datadict[cval]['maxval'])
                    if datadict[cval]['minval'] > date_time :
                        datadict[cval]['minval']=date_time
                    if datadict[cval]['maxval'] < date_time :
                        datadict[cval]['maxval']=date_time
                    datadict[cval]['diff']=datadict[cval]['maxval']-datadict[cval]['minval']         
                else:
                    datadict[cval]={ "minval":date_time,
                                     "maxval":date_time,
                                     "diff": date_time-date_time
                                    }
                
                 
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict

def readcsvdatascn(fname,row):
    totalerr=""
    datadict={}
    Masterlist=[]
    cvalold=-1
    i=0
    rval=0
    try:
        with open(fname, mode ='r')as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                if i==0 :
                    i=i+1
                    continue
                i=i+1
                if i>row :
                    break
                #print(lines)
                cval=lines[2]
                 
                 
                date_time = datetime.strptime(lines[4], '%d-%b-%y %I.%M.%S.%f000 %p')
                if cval in datadict:
                    if datadict[cval] > date_time :
                        datadict[cval]= date_time
                else:
                    datadict[cval]= date_time
                
                 
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict


mainProgram("sys1.csv","sys2.csv",290000,676,8)



