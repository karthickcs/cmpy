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
import logging
 
# Create and configure logger
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ',
                    handlers=[logging.FileHandler("logfile.log"),
                              logging.StreamHandler()])        
logger = logging.getLogger("ReadFromDb")
logger.setLevel(logging.DEBUG)   
logger.debug(os.path.abspath(os.getcwd()))
cd=os.path.abspath(os.getcwd())
cd=cd.replace('ReadFromDb','instantclient_21_14')
logger.debug(cd)
oracledb.init_oracle_client(lib_dir=cd)
# oracledb.init_oracle_client(lib_dir=r"C:\Users\cskar\Downloads\instantclient-basic-windows.x64-19.22.0.0.0dbru\instantclient_19_22")
 
def readfromtabledataGenerateMeta(tabdatold,tabdatnew,taskid,runid):
    totalerr=""
    
    con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
    cursor = con.cursor()
    try:
       
        datadict={}
        
        newtname = ''
        newrecord =''
        
        for keyold in  list(tabdatold.keys()):
            #logger.debug("Comparing --> Meta data -->"+keyold) 
            oldtname=tabdatold[keyold][0]
            oldrecord=tabdatold[keyold][1]
            newtname = ''
            newrecord =''
            oldjson=getfdataMetagen(oldrecord)
            if keyold in tabdatnew :
                newtname =  tabdatnew[keyold][0]
                newrecord = tabdatnew[keyold][1] 
                newjson=getfdataMetagen(newrecord)
                difference= list(dictdiffer.diff(oldjson, newjson))
            else :
                newjson={}
                difference={"tablemissing": "TargetSystem"}               
             
            
            insertProwsDifftabstruct(cursor,taskid,runid,oldtname, oldjson,newjson,difference)
            con.commit()
        for keynew in  list(tabdatnew.keys()):
            newtname=tabdatnew[keynew][0]
            newrecord=tabdatnew[keynew][1]
            oldtname = ''
            oldrecord =''
            found =''
            if keynew in tabdatold:
                continue 
             
            datadicttemp={}
             
            oldjson={}
            newjson=getfdataMetagen(newrecord)
            difference=difference={"tablemissing": "SrcSystem"}
            insertProwsDifftabstruct(cursor,taskid,runid,newtname, oldjson,newjson,difference)    
            con.commit()
    except Exception:
        traceback.print_exc()
    if cursor:
        cursor.close()
    if con:
        con.close()      
    print(totalerr)
    return datadict

def getfdataMetagen(text):
    mytemp={}
    idict={}
    errval=0
    datadict={}
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
                if path.find("c3") > -1 and  x.text and  x.text.isnumeric(): 
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
                            idict[x.attrib[aname]]=x.text
                if path.find("c4") > -1 and  x.text  : 
                    att=x.attrib.keys() 
                    if len(att) ==0 :
                        datadict[path]=x.text
                    else:
                        aname=x.attrib.keys()[0]
                        if aname.find("id") > -1:
                            datadict["tablename"]=x.attrib[aname]   
                        else:
                            try:        
                                datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                            except Exception:
                                errval=errval+1 
                if path.find("c6") > -1 and  x.text  : 
                    att=x.attrib.keys() 
                    if len(att) ==0 :
                        datadict[path]=x.text
                    else:
                        aname=x.attrib.keys()[0]
                        if aname.find("id") > -1:
                            datadict["tablename"]=x.attrib[aname]   
                        else:
                            try:        
                                datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                            except Exception:
                                errval=errval+1 
                if path.find("c10") > -1 and  x.text  : 
                    att=x.attrib.keys() 
                    if len(att) ==0 :
                        datadict[path]=x.text
                    else:
                        aname=x.attrib.keys()[0]
                        if aname.find("id") > -1:
                            datadict["tablename"]=x.attrib[aname]   
                        else:
                            try:        
                                datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                            except Exception:
                                errval=errval+1 
                            
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
        print()
    return datadict
 
        
def conecttoOracleGenerateMeta(user,password,jdbcurl,tname ):
    cursor = None
    con = None
    Mainsys = {}
    try:
        
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
     
        cursor = con.cursor() 
        statement="select recid,xmltype.getclobval(xmlrecord)  from  "  + tname   + "     order by recid "
        cursor.execute(statement)    		
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            logger.debug('No rows')
            return
        while (remaining_rows):
            sys =[]
            try:
               
                    sys.append(remaining_rows[0])
                    sys.append( remaining_rows[1].read())
                                  
                    Mainsys[remaining_rows[0]]=sys
            except Exception:
                logger.debug("error ") 
            remaining_rows = cursor.fetchone()
         
        return Mainsys
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close() 
 
def insertProwsDifftabstruct(cursor,taskid,runid,tname,oldjson,newjson,difference):
     
    
    try:
        oldjson_str = str(oldjson).replace("'","\"")
        newjson_str = str(newjson).replace("'","\"")
        difference_mylist = str(difference).replace("'","\"")
       
        statement="INSERT INTO  Diff_Table_struct (TASKID,runid, TNAME, TSTRUCTOLDSYS,TSTRUCTNEWSYS, DIFFERENCE ) VALUES ('"+str(taskid)+"','"+str(runid)+"','"+str(tname)+"', '"+str(oldjson_str)+"', '"+str(newjson_str)+"', '"+str(difference_mylist)+"')"
        cursor.execute(statement)    		
        #print (statement)
         
        
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++",str( e))
    
        
            
            

def updatedplistenstatus(taskid,runid,loadtime,batchtime,comaparetime,reporttime):
    cursor = None
    con = None
    
    try:
        
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement=" update dp_listen_table set  dataloadtime='"+str(loadtime)+"',batchtime='"+str(batchtime)+"',comparetime='"+str(comaparetime)+"',reportgentime='"+str(reporttime)+"'    where  taskid='"+ str(taskid)+"' and  runid='"+str(runid) + "' "
        cursor.execute(statement)    		
        #print (statement)
        
        con.commit()
        
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++",str( e))
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()          
            
          

def updaterowproceesed(cursor,taskid,runid ):
     
    
    try:
        
   
        statement=" update dp_listen_table set rowsprocessed=rowsprocessed+1     where  taskid='"+ str(taskid)+"' and  runid='"+str(runid) + "' "
        cursor.execute(statement)    		
        
        
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++",str( e))
                        
            
            
def deleteoldRec(taskid ):
    cursor = None
    con = None
    
    try:
        logger.debug("Delete old records")
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="delete  from  Diff_Table_struct where  taskid='"+ str(taskid)+"'   "
        cursor.execute(statement)    		
        #print (statement)
        
        con.commit()
        
    except psycopg2.DatabaseError as e:
        print("There is a problem with Oracle+++++++",str( e))
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()            
           
           
 
def insertProws(cursor,taskid,runid,differeceval,maintranid,comptranid,compscore,tid,sequence):
    
    
    try:
        json_mylist = str(differeceval).replace("'","\"")
        
        statement="INSERT INTO  DIFF_TABLE (TASKID,runid, INSERTTS, MAINTRANID, NEWTRANID, SEQUENCE, DIFFERENCE, COUNTVAL, THREADID) VALUES ('"+str(taskid)+"','"+str(runid)+"', Current_timestamp, '"+str(maintranid)+"', '"+str(comptranid)+"', '"+str(sequence)+"', '"+str(json_mylist)+"', '"+str(compscore)+"', '"+str(tid)+"')"
        cursor.execute(statement)    		
        #print (statement)
         
        
    except psycopg2.DatabaseError as e:
        logger.debug("There is a problem with Oracle+++++++",str( e))
    
        
           

def conecttoOraclecount(user,password,jdbcurl,tname,startts,endts,oldarr):
    cursor = None
    con = None
    
    try:
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        Mainsys=[]
        cursor = con.cursor()
        statement="SELECT count(*) FROM "  + tname   + " where TNAME not like '%_BATCH%' and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "
        cursor.execute(statement)    		
        #results = cursor.fetchone()
         
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            logger.debug('No rows')
            return 0;
        return  remaining_rows[0]
         
    except Exception:
        logger.error("exception ",exc_info=1)
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
            logger.debug('No rows')
            return
        while (remaining_rows):
            sys =[]
            try:
                if remaining_rows[0] in oldarr:
                    logger.debug("Already Processed"+ remaining_rows[0])
                else:
                    sys.append(remaining_rows[0])
                    sys.append(remaining_rows[1])
                    sys.append(remaining_rows[2])                  
                    Mainsys.append(sys)
            except Exception:
                logger.debug("error ") 
            remaining_rows = cursor.fetchone()
         
        return Mainsys
    except Exception:
        logger.error("exception ",exc_info=1)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()


def conecttoOracleOnline(user,password,jdbcurl,tname,startts,endts):
    cursor = None
    con = None
    Mainsys = []
    try:
        statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where TNAME not like '%_BATCH%'  and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') order by scnnum,updatets,tname"
        logger.debug(statement)
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        
        cursor = con.cursor()
        
        cursor.execute(statement) 
          		
        #results = cursor.fetchone()
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            logger.debug('No rows')
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
                logger.debug("error online ") 
            remaining_rows = cursor.fetchone()
        logger.debug('connected'+str(len(Mainsys))) 
        return Mainsys
    except Exception:
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
            
            
def conecttoOracleClob(user,password,jdbcurl,tname,startts,endts):
    cursor = None
    con = None
    Mainsys = []
    try:
        statement="select TNAME,ROWIDVAL,SCNNUM,information,UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') order by scnnum,updatets,tname"
        logger.debug(statement)
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        
        cursor = con.cursor()
        
        cursor.execute(statement) 
          		
        #results = cursor.fetchone()
        remaining_rows = cursor.fetchone()
        logger.debug('connected'+str(len(remaining_rows))) 
        if remaining_rows == None :
            logger.debug('No rows')
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
                logger.debug("error clob") 
            remaining_rows = cursor.fetchone()
        logger.debug('connected'+str(len(Mainsys))) 
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
        logger.debug(statement)
        con=oracledb.connect(
        user=user,
        password=password,
        dsn=jdbcurl)
        
        cursor = con.cursor()
        
        cursor.execute(statement) 
          		
        #results = cursor.fetchone()
        remaining_rows = cursor.fetchone()
         
        if remaining_rows == None :
            logger.debug('No rows')
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
                logger.debug("error clob") 
            remaining_rows = cursor.fetchone()
        logger.debug('connected----->'+str(len(Mainsys))) 
        return Mainsys
    except Exception:
        logger.error("exception ",exc_info=1)
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
        rowprocessed=0
        loadtime=0
        batchtime=0
        comaparetime=0
        reporttime=0 
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where  taskid='"+ oldarr[0]+"' and  runid='"+oldarr[1] + "'order by insertts"
        cursor.execute(statement)    		
        results = cursor.fetchone()
        columns=[column[0] for column in cursor.description]
        dd={}
        for i in range(len(results)):
            dd[columns[i]]=results[i]
        if results == None :
            return
        upstatement ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\''+str(dd['taskid']) +'\' and runid=\''+str(dd['runid']) +'\''
        #print (upstatement)
        cursor.execute(upstatement)
        con.commit()
        logger.debug(str(results))
        if dd['sys1type']  == "DB" :
            sys1=conecttoOracle(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],dd['tablenamesys1'],dd['starttssys1'],dd['endtssys1'],oldarr)
            sys2=conecttoOracle(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],dd['tablenamesys2'],dd['starttssys2'],dd['endtssys2'],oldarr)
            mainProgramDbApproach(sys1,sys2,results,dd['rowcount'],dd['taskid'],dd['runid'],"true")
        else :
            #mainProgram(results[2],results[8],results[6],results[1],results[7])
            logger.debug("hello")    
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(dd['taskid']) +'\' and  runid=\''+str(dd['runid']) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
        logger.debug("There is a problem with Oracle+++++++",str(e))
    except Exception:
        logger.error("exception ",exc_info=1)    
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
         
def tableListener():
    cursor = None
    con = None
    
    try:
        rowprocessed=0
        loadtime=0
        batchtime=5
        comaparetime=10
        reporttime=0
        start = time.time()
 

        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where status='CREATED'  and (select count(status) from DP_LISTEN_TABLE where status='INPROGRESS' ) = 0 order by insertts"
        cursor.execute(statement)    		
        results = cursor.fetchone()
        if results == None :
            return
        columns=[column[0] for column in cursor.description]
        dd={}
        for i in range(len(results)):
            dd[columns[i]]=results[i]
            
        
        upstatement ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\''+str(dd['taskid']) +'\' and runid=\''+str(dd['runid']) +'\''
        #print (upstatement)
        cursor.execute(upstatement)
        con.commit()
        logger.debug(str(results))
        df.to_csv('Finaldiff_'+ str(dd['taskid'])+'_'+str(dd['runid']) +'.csv', header='column_names')
        sys1=[]
        sys2=[]
        
        if dd['generate_metadata'] != None and dd['generate_metadata'] !='' and dd['generate_metadata'] =='true' :
            logger.debug("Started Meta data comparison") 
            deleteoldRec(dd['taskid'] )
            sys1Meta=conecttoOracleGenerateMeta(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],'F_STANDARD_SELECTION')
            sys2Meta=conecttoOracleGenerateMeta(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],'SS2')
            upstatement ="update DP_LISTEN_TABLE set ROWCOUNT='" +str(len(list(sys1Meta.keys())))  + "',ROWCOUNTSYS2='" + str(len(list(sys2Meta.keys())))  + "', UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\'"+str(dd['taskid']) +"\' and runid=\'"+str(dd['runid']) +"\'"              
            cursor.execute(upstatement)
            con.commit()
            done = time.time()
            loadtime =int(( done - start))+1
            start = time.time()
            updatedplistenstatus(dd['taskid'],-1 ,loadtime,batchtime,comaparetime,reporttime)
            ansold=readfromtabledataGenerateMeta(sys1Meta,sys2Meta,dd['taskid'],'1' )
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            updatedplistenstatus(dd['taskid'],-1 ,loadtime,batchtime,comaparetime,reporttime)
            logger.debug("Completed Meta data comparison") 
            upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(dd['taskid']) +'\' and  runid=\''+str(dd['runid']) +'\''
            #print (upstatement)
            cursor.execute(upstatement2)
            con.commit()
            return
        
        if dd['starttsonlinesys1'] != None and dd['starttsonlinesys1'] !='' and len(dd['starttsonlinesys1']) >0 :            
            sys1=conecttoOracleOnline(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],dd['tablenamesys1'],dd['starttsonlinesys1'],dd['endtsonlinesys1'])
            sys2=conecttoOracleOnline(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],dd['tablenamesys2'],dd['starttsonlinesys2'],dd['endtsonlinesys2'])
            done = time.time()
            loadtime =int(( done - start))+1
            updatedplistenstatus(dd['taskid'],dd['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            start = time.time()
            upstatement ="update DP_LISTEN_TABLE set ROWCOUNT='" +str(len(sys1))  + "',ROWCOUNTSYS2='" + str(len(sys2))  + "', UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\'"+str(dd['taskid']) +"\' and runid=\'"+str(dd['runid']) +"\'"              
            cursor.execute(upstatement)
            con.commit()
            dprocesmultiOnline('Onlineprocess',sys1,sys2,dd['taskid'],dd['runid'])
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            updatedplistenstatus(dd['taskid'],dd['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            # mainProgramDbApproach(sys1,sys2,results,dd['rowcount'],dd['taskid'],dd['runid'],"false")
            clearfile("cont.txt","")  
        
        if dd['starttsonlinesys1'] != None and dd['starttsonlinesys1'] !='' and len(dd['starttsonlinesys1']) >0 :              
                
            sys1=conecttoOracleClob(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],dd['tablenamesys1']+"_CLOB",dd['starttsonlinesys1'],dd['endtsonlinesys1'])
            sys2=conecttoOracleClob(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],dd['tablenamesys2']+"_CLOB",dd['starttsonlinesys2'],dd['endtsonlinesys2'])
            dprocesmultiClob('Clobprocess',sys1,sys2,dd['taskid'],dd['runid'])
            clearfile("cont.txt","")  
            # mainProgramDbApproach(sys1,sys2,results,dd['rowcount'],dd['taskid'],dd['runid'],"false")
        if dd['sys1type']  == "DB" :
            countsys1=conecttoOraclecount(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],dd['tablenamesys1'],dd['starttssys1'],dd['endtssys1'],[])
            countsys2=conecttoOraclecount(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],dd['tablenamesys2'],dd['starttssys2'],dd['endtssys2'],[])
            upstatement ="update DP_LISTEN_TABLE set ROWCOUNT='" +str(countsys1)  + "',ROWCOUNTSYS2='" + str(countsys2)  + "', UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\'"+str(dd['taskid']) +"\' and runid=\'"+str(dd['runid']) +"\'"              
            cursor.execute(upstatement)
            con.commit()   
            sys1=conecttoOracle(dd['usernamesys1'],dd['passwordsys1'],dd['jdbcurlsys1'],dd['tablenamesys1'],dd['starttssys1'],dd['endtssys1'],[])
            sys2=conecttoOracle(dd['usernamesys2'],dd['passwordsys2'],dd['jdbcurlsys2'],dd['tablenamesys2'],dd['starttssys2'],dd['endtssys2'],[])
            done = time.time()
            loadtime =int(( done - start))+1
            start = time.time()
            updatedplistenstatus(dd['taskid'],dd['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            mainProgramDbApproach(sys1,sys2,results,dd['rowcount'],dd['taskid'],dd['runid'],"false")
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            updatedplistenstatus(dd['taskid'],dd['runid'] ,loadtime,batchtime,comaparetime,reporttime)
        else :
            #mainProgram(results[2],results[8],results[6],results[1],results[7])
            logger.debug("hello")    
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(dd['taskid']) +'\' and  runid=\''+str(dd['runid']) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
        logger.debug("There is a problem with Oracle+++++++",str(e))
    except Exception:
        logger.error("exception ",exc_info=1)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

            
def reaxmldata(scndetails,astring,batch):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    cval=""
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
        logger.error("Exception"+batch)
        logger.error("exception ",exc_info=1)
    logger.debug(totalerr)
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
                sval=str(x.attrib).replace("{","[").replace("}","]")      
                datadict[tname+"::"+path]=x.text
    except Exception:
        err =1
     
    return datadict

def gettrantableNames(scndetails,astring,batch):
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
        logger.error("exception"+batch)
        logger.error("exception ",exc_info=1)
    return datadict

def innerthread(con,tid,start,end,tab1,tab2,tnameConcattab1,tnameConcattab2,tnamereversemap,taskid,runid):
    
    cursor = con.cursor()  
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
                insertProws(cursor,taskid,runid,list(list(dictdiffer.diff(master, tab2[matchkey]))),kvalmaster,matchkey,compscore,tid ,i)
            updaterowproceesed(cursor,taskid,runid)   
            con.commit()
    except Exception:
        logger.error("exception ",exc_info=1)
    if cursor:
        cursor.close()
       
    df = pd.DataFrame(Masterlist)
    appendfile("cont.txt",str(tid))
    ptimead(tid,'end')
    df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', mode='a', header=False)
    
    
  

def dprocesmulti(tid,scndetssys1,scndetssys2,taskid,runid):
    con = None
    try:
        #print ("start Inner Thread of ---->"+tid) 
        
        tab1=reaxmldata(scndetssys1,"",tid)
        tab2=reaxmldata(scndetssys2,"",tid)       
        tnameConcattab1=gettrantableNames(scndetssys1,"",tid)
        tnameConcattab2=gettrantableNames(scndetssys2,"",tid)       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=len(tab1)       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432) 
        if inrr==0:
            inrr=1
        for i in range(1):
            t = threading.Thread(target=innerthread,args=(con,tid,inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
           
            t.start()
            logger.error("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        logger.error("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger.error("exception ",exc_info=1)
    if con:      
        con.close()     
    
def dprocesmultiOnline(tid,scndetssys1,scndetssys2,taskid,runid):
    con =None
    try:
        #print ("start Inner Thread of ---->"+tid) 
        # overrite("cont.txt",str(taskid)+","+str(runid))
        tab1=reaxmldata(scndetssys1,"",tid)
        tab2=reaxmldata(scndetssys2,"",tid)       
        tnameConcattab1=gettrantableNames(scndetssys1,"",tid)
        tnameConcattab2=gettrantableNames(scndetssys2,"",tid)       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=100       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432) 
        inrr+=1
        for i in range(inrr):
            t = threading.Thread(target=innerthread,args=(con,tid+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
           
            t.start()
            logger.error("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        logger.error("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger.error("exception ",exc_info=1)
    if con:
        con.close()       

def getxmlfromclobdata(text):
    root = etree.Element("root")
    try:
    
        row = text.split("")
        root = etree.Element("root")
        child = etree.SubElement(root, "row")
        i=1
        for value in row:
            etree.SubElement(child, "pos"+str(i)).text = value
            i=i+1
    except:
        logger.error("exception ",exc_info=1)   
        
    #print (etree.tostring(root, encoding="unicode"))
    return etree.tostring(root, encoding='utf-8')
def reaxmldataforclob(scndetails,astring,batch):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    cval=""
    try:       
        for   row in scndetails:
        
            cval=row[0]+"|"+row[1]+"|"+astring+row[2]
            datadicttemp={}
            if(cval != cvalold) :
                rval=1;
                cvalold=cval            
            if cval in datadict:
                datadict[cval]=getxmlfromclobdata(row[3])
            else:
                datadict[cval]=getxmlfromclobdata(row[3])
            i=i+1
            rval=rval+1
    except Exception:
        logger.error("Exception"+batch)
        logger.error("exception ",exc_info=1)
    logger.debug(totalerr)
    return datadict
def dprocesmultiClob(tid,scndetssys1,scndetssys2,taskid,runid):
    try:
        #print ("start Inner Thread of ---->"+tid) 
        # overrite("cont.txt",str(taskid)+","+str(runid))
        tab1=reaxmldataforclob(scndetssys1,"",tid)
        tab2=reaxmldataforclob(scndetssys2,"",tid)       
        tnameConcattab1=gettrantableNames(scndetssys1,"",tid)
        tnameConcattab2=gettrantableNames(scndetssys2,"",tid)       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=100       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
         
        inrr+=1
        for i in range(inrr):
            t = threading.Thread(target=innerthread,args=(tid+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid))
           
            t.start()
            logger.error("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        logger.error("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger.error("exception ",exc_info=1)


def ptimead(tid,ty):
    logger.debug(tid)
 

def overrite(fname,text):
    try:
    # Write-Overwrites
        file1 = open(fname, "w") 
        file1.write(text+",")
        file1.close()    
    except Exception:
        logger.error("exception ",exc_info=1)

def clearfile(fname,text):
    try:
    # Write-Overwrites
        file1 = open(fname, "w") 
        file1.write(text)
        file1.close()    
    except Exception:
        logger.error("exception ",exc_info=1)
    
def appendfile(fname,text):
    try:
        file1 = open(fname, "a") # append mode
        file1.write(text+",")
        file1.close()
    except Exception:
        logger.error("exception ",exc_info=1)    
    
def readfile(fname):
    try:
        file1 = open(fname, "r")    
        dat= file1.read()
        file1.close() 
        return dat
    except Exception:
        logger.error("exception ",exc_info=1)
    return ""    


 
    

####################################################################
os.system('cls')
start = datetime.now(pytz.timezone('Asia/Kolkata'))
logger.debug("Start --->:")
 
threadrunner =[]
Masterlist=[] 
threadrunner =[]
df = pd.DataFrame(Masterlist)

def mainProgramDbApproach(sys1,sys2,results,rowcount,taskid,runid,cont):
    
    if cont == "false" :
        overrite("cont.txt",str(taskid)+","+str(runid))
       
    try:    
     
        for i in range(len(sys1)):
       
            batch=sys1[i][0]
            
            # if batch!= "US1/COB.INITIALISE" :
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
            if  scndetssys1 == None :
                logger.debug("Batch with zero rows ::"+batch)
                continue
            t = threading.Thread(target=dprocesmulti,args=(batch,scndetssys1,scndetssys2,taskid,runid))
            t.start()
            threadrunner.append(t)
            
                    
        for threadm in threadrunner :
            threadm.join()    
                
        logger.error("Exiting Main Thread") 
        end = datetime.now(pytz.timezone('Asia/Kolkata'))
        clearfile("cont.txt","")
        logger.debug("end :")
        logger.debug("Total time ===== :"+str(end-start))
        logger.debug('done')   
    except Exception:
        logger.error("exception ",exc_info=1)              



# a loop to run the check every 2 seconds- as to lessen cpu usage
def sleepLoop():
    while 0 == 0:
        tableListener()
        logger.debug("Listening ....**********----->")
        time.sleep(10.0)
os.system('cls')
logger.debug('hi')
readcont=readfile("cont.txt")

if readcont !=  "" :     
    inputval = input('OLd Run Exixt Press Y to Continue where you left \n , N to Proceed Normal Flow ...\n') 
    if inputval.upper() == "Y": 
        tableListenercontinue(readcont)
sleepLoop()
  

logger.debug('completed')
logger.debug('done')
 