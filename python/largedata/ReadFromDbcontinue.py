from datetime import datetime
import oracledb
import psycopg2
import os
from lxml import etree,objectify
import sys 
from operator import length_hint 
import dictdiffer 
import time
import pandas as pd
import traceback
from collections import defaultdict
import pytz
from threading import Thread
import threading
           
print(os.path.abspath(os.getcwd()))
cd=os.path.abspath(os.getcwd())
cd=cd.replace('ReadFromDb','instantclient_21_14')
print(cd)
oracledb.init_oracle_client(lib_dir=cd)
# oracledb.init_oracle_client(lib_dir=r"C:\Users\cskar\Downloads\instantclient-basic-windows.x64-19.22.0.0.0dbru\instantclient_19_22")

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
            
            
 
def conecttoOracle(user,password,jdbcurl,tname,startts,endts,oldarr):
    cursor = None
    con = None
    
    try:
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        Mainsys=[]
        cursor = con.cursor()
        statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%' GROUP BY ROWIDVAL"
        cursor.execute(statement)    		
        #results = cursor.fetchone()
         
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            print('No rows')
            return
        while (remaining_rows):
            sys =[]
            try:
                if remaining_rows[0] in oldarr:
                    print("Already Processed"+ remaining_rows[0])
                else:
                    sys.append(remaining_rows[0])
                    sys.append(remaining_rows[1])
                    sys.append(remaining_rows[2])                  
                    Mainsys.append(sys)
            except Exception:
                print("error ") 
            remaining_rows = cursor.fetchone()
         
        return Mainsys
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()



def conecttoOraclefbatch(user,password,jdbcurl,tname,startts,endts):
    cursor = None
    con = None
    Mainsys = []
    try:
        statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6') order by scnnum,updatets,tname"
        print(statement)
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        
        cursor = con.cursor()
        
        cursor.execute(statement) 
          		
        #results = cursor.fetchone()
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            print('No rows')
            return
        while (remaining_rows):
            sys =[]
            try:
                sys.append(remaining_rows[0])
                sys.append(remaining_rows[1])
                sys.append(remaining_rows[2])            
                sys.append ( remaining_rows[3].read())
                sys.append(remaining_rows[4])
                Mainsys.append(sys)
            except Exception:
                print("error clob") 
            remaining_rows = cursor.fetchone()
        print('connected----->'+str(len(Mainsys))) 
        return Mainsys
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
   

def tableListenercontinue(txt):
    cursor = None
    con = None
    
    try:
        oldarr = txt.split(",")
         
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where  taskid='"+ oldarr[0]+"' and  runid='"+oldarr[1] + "'order by insertts"
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
            sys1=conecttoOracle(results[11],results[12],results[10],results[14],results[15],results[16],oldarr)
            sys2=conecttoOracle(results[18],results[19],results[17],results[21],results[22],results[23],oldarr)
            mainProgramDbApproach(sys1,sys2,results,results[6],results[1],results[7],"true")
        else :
            #mainProgram(results[2],results[8],results[6],results[1],results[7])
            print("hello")    
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(results[1]) +'\' and  runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    except Exception:
        traceback.print_exc()    
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
         
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
            sys1=conecttoOracle(results[11],results[12],results[10],results[14],results[15],results[16],[])
            sys2=conecttoOracle(results[18],results[19],results[17],results[21],results[22],results[23],[])
            mainProgramDbApproach(sys1,sys2,results,results[6],results[1],results[7],"false")
        else :
            #mainProgram(results[2],results[8],results[6],results[1],results[7])
            print("hello")    
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(results[1]) +'\' and  runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

            
def reaxmldata(scndetails,astring):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    try:       
        for   row in scndetails:
        
            cval=row[0]+"|"+row[1]+"|"+astring+row[2]
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
                cvalold=cval            
            if cval in datadict:
                datadict[cval]=getfdata(row[3],datadict[cval],row[0],cval,i,astring+row[1])
            else:
                datadict[cval]=getfdata(row[3],datadicttemp,row[0],cval,i,astring+row[1])
            i=i+1
            rval=rval+1
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict

def getfdata(text,datadict,tname, kkey, rowid ,recid):

    try:
        ccc=0
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
                datadict[tname+"::"+path]=x.text
    except Exception:
        ii=1
     
    return datadict

def gettrantableNames(scndetails,astring):
    totalerr=""
    datadict={}     
    i=0
    rval=0
    try:
        previous=0
        i=0
        concatstr =""
        for   row in scndetails:
            cval=astring+row[2]           
        
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
            keysListtab2 = list(tab2.keys())
            for kval in keysListtab2:
                
                if (kvalmaster.split("|")[0] != kval.split("|")[0]):
                    continue
                keysListtabtan2=tnamereversemap[tnameConcattab1[kvalmaster.split("|")[2]]]
                if kval.split("|")[2] not in keysListtabtan2:
                    continue
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
    appendfile("cont.txt",str(tid))
    ptimead(tid,'end')
    df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', mode='a', header=False)
    
    
    
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
    df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', mode='a', header=False)



def dprocesmulti(tid,scndetssys1,scndetssys2,taskid,runid):
    try:
        #print ("start Inner Thread of ---->"+tid) 
        ptime()
        tab1=reaxmldata(scndetssys1,"")
        tab2=reaxmldata(scndetssys2,"s2.")       
        tnameConcattab1=gettrantableNames(scndetssys1,"")
        tnameConcattab2=gettrantableNames(scndetssys2,"s2.")       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=len(tab1)       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
        if inrr==0:
            inrr=1
        for i in range(1):
            t = threading.Thread(target=innerthread,args=(tid,inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
           
            t.start()
            print ("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        print ("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        traceback.print_exc()
    


def ptimead(tid,ty):
    print(ty+'  time -->',datetime.now(pytz.timezone('Asia/Kolkata')),tid)
    
def ptime():
    print('  time -->',datetime.now(pytz.timezone('Asia/Kolkata')))    


def overrite(fname,text):
    # Write-Overwrites
    file1 = open(fname, "w") 
    file1.write(text+",")
    file1.close()    
    
def appendfile(fname,text):
    file1 = open(fname, "a") # append mode
    file1.write(text+",")
    file1.close()
    
def readfile(fname):
    file1 = open(fname, "r")    
    dat= file1.read()
    file1.close() 
    return dat


 
    

####################################################################
os.system('cls')
start = datetime.now(pytz.timezone('Asia/Kolkata'))
print("Start --->:", start)
ptime() 
threadrunner =[]
Masterlist=[] 
threadrunner =[]
df = pd.DataFrame(Masterlist)

def mainProgramDbApproach(sys1,sys2,results,rowcount,taskid,runid,cont):
    df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', header='column_names')
    if cont == "false" :
        overrite("cont.txt",str(taskid)+","+str(runid))
    try:    
     
        for i in range(len(sys1)):
       
            batch=sys1[i][0]
            
            # if batch!= "BNK/AC.STMT.UPDATE" :
            #     continue     
            mintime=sys1[i][1]
            maxtime=sys1[i][2]
            scndetssys1=conecttoOraclefbatch(results[11],results[12],results[10],results[14],mintime,maxtime)
             
            #scndetssys1 = sys1df.loc[(sys1df['UPDATETS']> datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S.%f')) & (sys1df['UPDATETS']< datetime.strptime(maxtime, '%Y-%m-%d %H:%M:%S.%f')) & (sys2df['TNAME']!='UPDATING on F_LOCKING')] 
            for row in sys2 :
                if row[0]== batch :                     
                    batchsys2=row[0]
                    mintimesys2=row[1]
                    maxtimesys2= row[2]
            scndetssys2=conecttoOraclefbatch(results[18],results[19],results[17],results[21],mintimesys2,maxtimesys2)
            
            t = threading.Thread(target=dprocesmulti,args=(batch,scndetssys1,scndetssys2,taskid,runid))
            t.start()
            threadrunner.append(t)
            
                    
        for threadm in threadrunner :
            threadm.join()    
                
        print ("Exiting Main Thread") 
        end = datetime.now(pytz.timezone('Asia/Kolkata'))
        overrite("cont.txt","")
        print("end :", end)
        print("Total time ===== :",end-start)
        print('done')   
    except Exception:
        traceback.print_exc()              



# a loop to run the check every 2 seconds- as to lessen cpu usage
def sleepLoop():
    while 0 == 0:
        tableListener()
        print("Listening ....**********----->")
        time.sleep(10.0)
os.system('cls')
print('hi')
readcont=readfile("cont.txt")
if readcont !=  "" :     
    inputval = input('OLd Run Exixt Press Y to Continue where you left \n , N to Proceed Normal Flow ...\n') 
    if inputval.upper() == "Y": 
        tableListenercontinue(readcont)
sleepLoop()
  

print('completed')
print('done')
 