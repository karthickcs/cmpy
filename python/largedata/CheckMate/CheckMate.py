import datetime
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

import DiffTableDao
import DiffTablestructDao
import DplistenDao
import FileProcessor
import IgnoreTableDao
import MetaDataGen
import OracleDao
import XmlProcessor
 
 
SuperArray=[]
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ')
    fileHandler = logging.FileHandler(log_file, mode='a')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)    


setup_logger('log1', "logfile.log",logging.INFO)
setup_logger('errorlog', "errrorfile.log",logging.DEBUG)
setup_logger('timelog', "timefile.log",logging.DEBUG)

logger_info = logging.getLogger('log1')
logger_debug = logging.getLogger('errorlog')
logger_time = logging.getLogger('timelog')
 
cd=os.path.abspath(os.getcwd())
cd=cd.replace('CheckMate','instantclient_21_14')
logger_info.info(cd)
logger_debug.info(cd)
oracledb.init_oracle_client(lib_dir=cd)
# oracledb.init_oracle_client(lib_dir=r"C:\Users\cskar\Downloads\instantclient-basic-windows.x64-19.22.0.0.0dbru\instantclient_19_22")
 
            
  
def tableListenercontinue(txt):
   

    try:
        oldarr = txt.split(",")
        rowprocessed=0
        loadtime=0
        batchtime=0
        comaparetime=0
        reporttime=0 
        dplistenDaoObj = DplistenDao.DplistenDao(
        logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
        )
        diffTableDaoObj = DiffTableDao.DiffTableDao(
        logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
        ) 
        diffTablestructObj = DiffTablestructDao.DiffTablestructDao(
         logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
        )
        
        ignoretableDaoObj = IgnoreTableDao.IgnoreTableDao(
         logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
         )
        processRow = dplistenDaoObj.getRowForPrpcessingwithId(oldarr[0],oldarr[1])
        if processRow == None :
            return   
        columns=[column[0] for column in dplistenDaoObj.cursor.description]
        columnLookUp={}
        for i in range(len(processRow)):
            columnLookUp[columns[i]]=processRow[i]
        dplistenDaoObj.updateStatus(columnLookUp['taskid'],columnLookUp['runid'],'INPROGRESS')
        logger_info.info(str(processRow))
        oracleDaosys1 = OracleDao.OracleDao(logger_info,logger_debug, columnLookUp['usernamesys1'],columnLookUp['passwordsys1'],columnLookUp['jdbcurlsys1'])
        oracleDaosys2 = OracleDao.OracleDao(logger_info,logger_debug, columnLookUp['usernamesys2'],columnLookUp['passwordsys2'],columnLookUp['jdbcurlsys2'])
        realtname=oracleDaosys2.oracleGetRealTnameType()
        difftableStruct=diffTablestructObj.getTableStructure(columnLookUp['taskid'])
        ignoretable=ignoretableDaoObj.getIgnoreList(columnLookUp['taskid'])
        if columnLookUp['sys1type']  == "DB" :
            sys1=oracleDaosys1.readFromTriggerTableCont(columnLookUp['tablenamesys1'] ,columnLookUp['starttssys1'],columnLookUp['endtssys1'],oldarr)
            sys2=oracleDaosys2.readFromTriggerTableCont(columnLookUp['tablenamesys2'] ,columnLookUp['starttssys2'],columnLookUp['endtssys2'],oldarr)
            compareDataForBatchApproach(oracleDaosys1,oracleDaosys2,dplistenDaoObj,diffTableDaoObj,sys1,sys2,processRow,columnLookUp['rowcount'],columnLookUp['taskid'],columnLookUp['runid'],"false",columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'],realtname,ignoretable,difftableStruct)
            
        else :
            
            logger_info.info("End")    
        dplistenDaoObj.updateStatusdone(columnLookUp['taskid'],columnLookUp['runid'],'DONE')
    except psycopg2.DatabaseError as e:
        logger_debug.debug("There is a problem with Oracle+++++++",str(e))
    except Exception:
        logger_debug.debug("exception ",exc_info=1)    
  
         
def tableListener():
   
    
    try:
        rowprocessed=0
        loadtime=0
        batchtime=5
        comaparetime=10
        reporttime=0
        start = time.time()
        dplistenDaoObj = DplistenDao.DplistenDao(
        logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
        )
        diffTableDaoObj = DiffTableDao.DiffTableDao(
        logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
        )
       
        diffTablestructObj = DiffTablestructDao.DiffTablestructDao(
         logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
         )
        
        ignoretableDaoObj = IgnoreTableDao.IgnoreTableDao(
         logger_info,logger_debug, "postgres", "postgres", "password", "localhost", 5432
         )
        
        metaDataGenObj = MetaDataGen.MetaDataGen()
       
           		
        processRow = dplistenDaoObj.getRowForPrpcessing()
        if processRow == None :
            return        
        
        columns=[column[0] for column in dplistenDaoObj.cursor.description]
        columnLookUp={}
        for i in range(len(processRow)):
            columnLookUp[columns[i]]=processRow[i]
            
        dplistenDaoObj.updateStatus(columnLookUp['taskid'],columnLookUp['runid'],'INPROGRESS'+sys.argv[1])    
        if columnLookUp['sys1type'] != None and columnLookUp['sys1type'] !='' and columnLookUp['sys1type'] =='FILE' :
            
            
            
            fileProcessor = FileProcessor.FileProcessor(logger_info,logger_debug) 
            fileProcessor.processFiles(columnLookUp['filelocation'],columnLookUp['filelocationt2'],dplistenDaoObj,diffTableDaoObj,columnLookUp['taskid'],columnLookUp['runid']) 
            dplistenDaoObj.updateStatusdone(columnLookUp['taskid'],columnLookUp['runid'],'DONE')
            return    
            
        
        oracleDaosys1 = OracleDao.OracleDao(logger_info,logger_debug, columnLookUp['usernamesys1'],columnLookUp['passwordsys1'],columnLookUp['jdbcurlsys1'])
        oracleDaosys2 = OracleDao.OracleDao(logger_info,logger_debug, columnLookUp['usernamesys2'],columnLookUp['passwordsys2'],columnLookUp['jdbcurlsys2'])
        logger_info.info(str(processRow))
        df.to_csv('Finaldiff_'+ str(columnLookUp['taskid'])+'_'+str(columnLookUp['runid']) +'.csv', header='column_names')
        sys1=[]
        sys2=[]
        realtname=oracleDaosys2.oracleGetRealTnameType()
        difftableStruct=diffTablestructObj.getTableStructure(columnLookUp['taskid'])
        ignoretable=ignoretableDaoObj.getIgnoreList(columnLookUp['taskid'])
        
        
      
        #  GenerateMetadata
        if columnLookUp['generate_metadata'] != None and columnLookUp['generate_metadata'] !='' and columnLookUp['generate_metadata'] =='true' :
            logger_info.info("Started Meta data comparison") 
            diffTablestructObj.deleteoldRecords(columnLookUp['taskid'] )
            sys1Meta=oracleDaosys1.GetMetaData('F_STANDARD_SELECTION')
            sys2Meta=oracleDaosys2.GetMetaData( 'F_STANDARD_SELECTION')
            dplistenDaoObj.updateRowCount(columnLookUp['taskid'],columnLookUp['runid'],len(list(sys1Meta.keys())),len(list(sys2Meta.keys())))
            
            done = time.time()
            loadtime =int(( done - start))+1
            start = time.time()
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],-1 ,loadtime,batchtime,comaparetime,reporttime)
            ansold=metaDataGenObj.generateMetaDataDifference(diffTablestructObj,sys1Meta,sys2Meta,columnLookUp['taskid'],'1',realtname )
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],-1 ,loadtime,batchtime,comaparetime,reporttime)
            logger_info.info("Completed Meta data comparison") 
            dplistenDaoObj.updateStatusdone(columnLookUp['taskid'],columnLookUp['runid'],'DONE')
            return
        # Online Processing
        if columnLookUp['starttsonlinesys1'] != None and columnLookUp['starttsonlinesys1'] !='' and len(columnLookUp['starttsonlinesys1']) >0 :            
            sys1=oracleDaosys1.readFromTriggerTable(columnLookUp['tablenamesys1'],columnLookUp['starttsonlinesys1'],columnLookUp['endtsonlinesys1'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            sys2=oracleDaosys2.readFromTriggerTable(columnLookUp['tablenamesys2'],columnLookUp['starttsonlinesys2'],columnLookUp['endtsonlinesys2'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            done = time.time()
            loadtime =int(( done - start))+1
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],columnLookUp['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            start = time.time()
            dplistenDaoObj.updateRowCount(columnLookUp['taskid'],columnLookUp['runid'],len(sys1),len(sys2))
            compareDataForOnline(dplistenDaoObj,diffTableDaoObj ,'Onlineprocess',sys1,sys2,columnLookUp['taskid'],columnLookUp['runid'],realtname,ignoretable,difftableStruct)
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],columnLookUp['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            
            clearfile("cont.txt","")  
          # Clob Processing
        if columnLookUp['starttsonlinesys1'] != None and columnLookUp['starttsonlinesys1'] !='' and len(columnLookUp['starttsonlinesys1']) >0 :              
                
            sys1=oracleDaosys1.readClobTriggerTable(columnLookUp['tablenamesys1']+"_CLOB",columnLookUp['starttsonlinesys1'],columnLookUp['endtsonlinesys1'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            sys2=oracleDaosys2.readClobTriggerTable(columnLookUp['tablenamesys2']+"_CLOB",columnLookUp['starttsonlinesys2'],columnLookUp['endtsonlinesys2'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            compareDataForClob(dplistenDaoObj,diffTableDaoObj ,'Clobprocess',sys1,sys2,columnLookUp['taskid'],columnLookUp['runid'] ,realtname)
            clearfile("cont.txt","")  
           # Batch  Processing   
        if columnLookUp['starttssys1'] != None and columnLookUp['starttssys1'] !='' and len(columnLookUp['starttssys1']) >0 :   
            countsys1=oracleDaosys1.countRows(columnLookUp['tablenamesys1'],columnLookUp['starttssys1'],columnLookUp['endtssys1'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            countsys2=oracleDaosys2.countRows(columnLookUp['tablenamesys2'],columnLookUp['starttssys2'],columnLookUp['endtssys2'],columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'])
            dplistenDaoObj.updateRowCount(columnLookUp['taskid'],columnLookUp['runid'], countsys1,countsys2)
           
            sys1=oracleDaosys1.readFromTriggerTableCont(columnLookUp['tablenamesys1'] ,columnLookUp['starttssys1'],columnLookUp['endtssys1'],[])
            sys2=oracleDaosys2.readFromTriggerTableCont(columnLookUp['tablenamesys2'] ,columnLookUp['starttssys2'],columnLookUp['endtssys2'],[])
            done = time.time()
            loadtime =int(( done - start))+1
            start = time.time()
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],columnLookUp['runid'] ,loadtime,batchtime,comaparetime,reporttime)
            if columnLookUp['tablewildcardallow'] == 'SPEED':
                compareDataForBatchSpeedApproach(oracleDaosys1,oracleDaosys2,dplistenDaoObj,diffTableDaoObj,sys1,sys2,processRow,columnLookUp['rowcount'],columnLookUp['taskid'],columnLookUp['runid'],"false",columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'],realtname,ignoretable,difftableStruct)
            else:
                compareDataForBatchApproach(oracleDaosys1,oracleDaosys2,dplistenDaoObj,diffTableDaoObj,sys1,sys2,processRow,columnLookUp['rowcount'],columnLookUp['taskid'],columnLookUp['runid'],"false",columnLookUp['tablewildcard'],columnLookUp['tablewildcardallow'],realtname,ignoretable,difftableStruct)
            done = time.time()
            comaparetime =int(( done - start))+1
            reporttime=1
            dplistenDaoObj.updateProcessTime(columnLookUp['taskid'],columnLookUp['runid'] ,loadtime,batchtime,comaparetime,reporttime)
        else :
            #mainProgram(processRow[2],processRow[8],processRow[6],processRow[1],processRow[7])
            logger_info.info("hello")    
        dplistenDaoObj.updateStatusdone(columnLookUp['taskid'],columnLookUp['runid'],'DONE')
    except psycopg2.DatabaseError as e:
        logger_debug.debug("There is a problem with Oracle+++++++",str(e))
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
     
         
 

def gettrantableNames(scndetails ,batch):
    totalerr=""
    datadict={}     
    i=0
    rval=0
    try:
        previous=0
        i=0
        concatstr =""
        scndetailssorted=sorted(scndetails, key=lambda x: (x[2], x[4] , x[0]), reverse=True)
        for   row in scndetailssorted:
            cval=row[2]           
        
            if cval != previous:
                concatstr =row[0]
                datadict[cval]=concatstr
                
            else:
                concatstr=concatstr+row[0]
                datadict[cval]=concatstr
            i=i+1
            previous=cval                
    except Exception:
        logger_debug.debug("exception"+batch)
        logger_debug.debug("exception ",exc_info=1)
    return datadict

def compareDataForOnlinethread(dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid,start,end,tab1,tab2,tnameConcattab1,tnameConcattab2,tnamereversemap,taskid,runid,realtname):
     
    try:  
       
        Masterlist=[]
        i=0 
        ptimead(tid,'start')
        keysListtab1 = list(tab1.keys())
        for kvalmaster in keysListtab1[start:end] :
            i=i+1             
            sublist=[]
            compscore=sys.maxsize
            matchkey=0
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
            SuperArray.append(kvalmaster)
            if compscore > 0:
               
                tnamevall=kvalmaster.split("|")[0].split(" ")[2]
                substring_start = tnamevall[:tnamevall.find('_')+1]
                substring_end = tnamevall[tnamevall.find('_')+1:]
                if substring_end in realtname:
                    realtnameResult=realtname[substring_end]
                else:
                    realtnameResult=substring_end    
                oname=""
                otype=""
                if realtnameResult :
                    oname=tnamevall
                    otype=realtnameResult[1]
                    odesc=realtnameResult[2]               
                diffTableDaoObj.insertRecords(taskid,runid,list(list(dictdiffer.diff(master, tab2[matchkey]))),kvalmaster,matchkey,compscore,tid ,i,oname,otype,odesc)
                diffTableDaoObj.prrowconnection.commit()
            if i%100 ==0 :    
                dplistenDaoObj.updaterowproceesed(taskid,runid,100)   
                dplistenDaoObj.prrowconnection.commit();
                
        dplistenDaoObj.updaterowproceesed(taskid,runid,i%100)   
        dplistenDaoObj.prrowconnection.commit();    
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
         
    df = pd.DataFrame(Masterlist)
    appendfile("cont.txt",str(tid))
    ptimead(tid,'end')
    df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', mode='a', header=False)
    
    
 
       
    
def compareDataForOnline(dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct):
     
    try:
        xmlProcessor = XmlProcessor.XmlProcessor(logger_info,logger_debug)
        #print ("start Inner Thread of ---->"+tid) 
        # overrite("cont.txt",str(taskid)+","+str(runid))
        tab1=xmlProcessor.getXMLData(scndetssys1 ,tid,realtname,ignoretable,difftableStruct,"src",taskid,runid,SuperArray)
        tab2=xmlProcessor.getXMLData(scndetssys2 ,tid,realtname,ignoretable,difftableStruct,"target",taskid,runid,SuperArray)       
        tnameConcattab1=gettrantableNames(scndetssys1 ,tid)
        tnameConcattab2=gettrantableNames(scndetssys2 ,tid)       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=100       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
        dplistenDaoObj.connectprrow()
        diffTableDaoObj.connectprrow()
        inrr+=1
        for i in range(inrr):
            t = threading.Thread(target=compareDataForOnlinethread,args=(dplistenDaoObj,diffTableDaoObj,tid+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid,realtname))
           
            t.start()
            logger_info.info("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        logger_info.info("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
    finally:
        dplistenDaoObj.closeprrowConnection(); 
        diffTableDaoObj.closeprrowConnection()   
 


def compareDataForClob(dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid,scndetssys1,scndetssys2,taskid,runid,realtname):
    try:
        xmlProcessor = XmlProcessor.XmlProcessor(logger_info,logger_debug)
        #print ("start Inner Thread of ---->"+tid) 
        # overrite("cont.txt",str(taskid)+","+str(runid))
        tab1=xmlProcessor.getXMLDataforclob(scndetssys1 ,tid)
        tab2=xmlProcessor.getXMLDataforclob(scndetssys2 ,tid)       
        tnameConcattab1=gettrantableNames(scndetssys1 ,tid)
        tnameConcattab2=gettrantableNames(scndetssys2 ,tid)       
        dres = defaultdict(list)
        dplistenDaoObj.connectprrow()
        diffTableDaoObj.connectprrow()
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=1000       
        inrval=0
        inrr=int(len(tab1)/intriger)
        inthreadrunner=[]
         
        inrr+=1
        for i in range(inrr):
            t = threading.Thread(target=compareDataForOnlinethread,args=(dplistenDaoObj,diffTableDaoObj,tid+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid,realtname))
           
            t.start()
            logger_info.info("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        
        logger_info.info("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
    finally:
        dplistenDaoObj.closeprrowConnection(); 
        diffTableDaoObj.closeprrowConnection()   

                                
def compareDataForBatchOneCycle(dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct):
    con = None
    try:
        logger_time.debug("start of Batch of ---->"+tid +"--->size"+str(len(scndetssys1)))
        start = datetime.datetime.now(pytz.timezone('Asia/Kolkata')) 
        # print ("start Inner Thread of ---->"+tid +"size"+str(len(scndetssys1))) 
        xmlProcessor = XmlProcessor.XmlProcessor(logger_info,logger_debug)
        tab1=xmlProcessor.getXMLData(scndetssys1 ,tid,realtname,ignoretable,difftableStruct,"src",taskid,runid,SuperArray)
        tab2=xmlProcessor.getXMLData(scndetssys2 ,tid,realtname,ignoretable,difftableStruct,"target",taskid,runid,SuperArray)     
        logger_info.info ("start Inner Thread of ---->"+tid +"size"+str(len(tab1)))   
        tnameConcattab1=gettrantableNames(scndetssys1 ,tid)
        tnameConcattab2=gettrantableNames(scndetssys2 ,tid)     
       
        dres = defaultdict(list)
        for key, val in sorted(tnameConcattab2.items()):
            dres[val].append(key)         
        tnamereversemap=dict(dres) 
        intriger=len(tab1)   
        intriger=1000      
        inrval=0
        inrr=int(len(tab1)/intriger)
        inrr+=1
        inthreadrunner=[]
        # con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432) 
     
        for i in range(inrr):
            t = threading.Thread(target=compareDataForOnlinethread,args=(dplistenDaoObj,diffTableDaoObj,tid+str(i),inrval,inrval+intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid,realtname))
                                         
            t.start()
            logger_debug.debug("start Batch  Thread of "+tid +"-->"+str(i)) 
            inthreadrunner.append(t)
            inrval=inrval+intriger
            
        for threadm in inthreadrunner :
            threadm.join()    
        # compareDataForOnlinethread(dplistenDaoObj,diffTableDaoObj,tid,inrval,intriger, tab1, tab2, tnameConcattab1 , tnameConcattab2,tnamereversemap,taskid,runid,realtname)
           
           
             
        end = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))  
        logger_time.debug("End of Batch of ---->"+tid  + "--Time taken -"+ str(end-start)  )
        logger_info.info("Exiting Batch  Thread of  Thread of "+tid) 
        
        
       
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
        logger_time.debug(tid+"exception ",exc_info=1)
        logger_time.debug("After Exception End of Batch of ---->"+tid    )
    

def ptimead(tid,ty):
    logger_info.info(tid)
 

def overrite(fname,text):
    try:
    # Write-Overwrites
        file1 = open(fname, "w") 
        file1.write(text+",")
        file1.close()    
    except Exception:
        logger_debug.debug("exception ",exc_info=1)

def clearfile(fname,text):
    try:
    # Write-Overwrites
        file1 = open(fname, "w") 
        file1.write(text)
        file1.close()    
    except Exception:
        logger_debug.debug("exception ",exc_info=1)
    
def appendfile(fname,text):
    try:
        file1 = open(fname, "a") # append mode
        file1.write(text+",")
        file1.close()
    except Exception:
        logger_debug.debug("exception ",exc_info=1)    
    
def readfile(fname):
    try:
        file1 = open(fname, "r")    
        dat= file1.read()
        file1.close() 
        return dat
    except Exception:
        logger_debug.debug("No cont file")
    return ""    


 
    

####################################################################
os.system('cls')
start = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
logger_info.info("Start --->:")
 
threadrunner =[]
Masterlist=[] 
threadrunner =[]
df = pd.DataFrame(Masterlist)

def compareDataForBatchApproach(oracleDaosys1,oracleDaosys2,dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,sys1,sys2,processRow,rowcount,taskid,runid,cont,tablewildcard,tablewildcardallow,realtname,ignoretable,difftableStruct):
    
    if cont == "false" :
        overrite("cont.txt",str(taskid)+","+str(runid))
    threadrunner =[]   
    try:    
        counter=0
        totalrowss1=0
        totalrowss2=0
        dplistenDaoObj.connectprrow()
        diffTableDaoObj.connectprrow()  
        for i in range(len(sys1)):
            try:
       
                batch=sys1[i][0]
                
                # if batch!= "BE1/AC.STMT.UPDATE" :
                #     continue     
                mintime=sys1[i][1]
                maxtime=sys1[i][2]
                scndetssys1=oracleDaosys1.fbatchRangeSearch(batch, processRow[14],mintime,maxtime,tablewildcard,tablewildcardallow)
                if scndetssys1 == None or len(scndetssys1) ==0:
                    # counter=counter+1  
                    logger_info.info("oldsys Batch with zero rows ::"+batch)
                    continue;
                totalrowss1=totalrowss1+len(scndetssys1)
                logger_info.info("oldsys Batch with  rows ::"+batch+"::"+str(len(scndetssys1))) 
                #scndetssys1 = sys1df.loc[(sys1df['UPDATETS']> datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S.%f')) & (sys1df['UPDATETS']< datetime.strptime(maxtime, '%Y-%m-%d %H:%M:%S.%f')) & (sys2df['TNAME']!='UPDATING on F_LOCKING')] 
                for row in sys2 :
                    if row[0]== batch :                     
                        batchsys2=row[0]
                        mintimesys2=row[1]
                        maxtimesys2= row[2]
                        batchsys2=row[0]
                scndetssys2=oracleDaosys2.fbatchRangeSearch(batchsys2, processRow[21],mintimesys2,maxtimesys2,tablewildcard,tablewildcardallow)
                if  scndetssys2 == None :
                    logger_info.info("newsys Batch with zero rows ::"+batch)
                    # counter=counter+1  
                    continue
                # compareDataForBatchOneCycle(dplistenDaoObj,diffTableDaoObj,batch,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct)
                counter=counter+1
                totalrowss2=totalrowss2+len(scndetssys2)
                 
                if counter%int(sys.argv[2]) != int(sys.argv[1]):
                    # logger_time.info("Batch skipped:"+batch +":"+str(counter)+":App:"+ str(sys.argv[1]) + "::size:"+ str(len(scndetssys1))) 
                    continue
                logger_time.info("Batch processed:"+batch +":"+str(counter)+":App:"+ str(sys.argv[1]) + "::size:"+ str(len(scndetssys1))) 
                t = threading.Thread(target=compareDataForBatchOneCycle,args=(dplistenDaoObj,diffTableDaoObj,batch,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct))
                t.start()                 
                threadrunner.append(t)
              
                logger_info.info("Batch processed Success ::  "+batch  + "   index of batch===>"+str(counter))
            
            except Exception:
                logger_debug.debug("exception "+ batch,exc_info=1)        
            # counter=counter+1  
        # dplistenDaoObj.updateRowCount(taskid,runid, totalrowss1,totalrowss2)          
        for threadm in threadrunner :
            threadm.join()    
                
        logger_info.info("Exiting Main Thread") 
        end = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        clearfile("cont.txt","")
        logger_info.info("end :")
        logger_info.info("Total time ===== :"+str(end-start))
        logger_info.info('done')   
    except Exception:
        logger_debug.debug("exception ",exc_info=1)              
    finally:
        dplistenDaoObj.closeprrowConnection(); 
        diffTableDaoObj.closeprrowConnection()    
        
        
        
        
    
def compareDataForBatchSpeedApproach(oracleDaosys1,oracleDaosys2,dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,sys1,sys2,processRow,rowcount,taskid,runid,cont,tablewildcard,tablewildcardallow,realtname,ignoretable,difftableStruct):
    
    if cont == "false" :
        overrite("cont.txt",str(taskid)+","+str(runid))
    threadrunner =[]   
    try:    
        counter=0
        totalrowss1=0
        totalrowss2=0
        dplistenDaoObj.connectprrow()
        diffTableDaoObj.connectprrow()  
        for i in range(len(sys1)):
            try:
                if counter%int(sys.argv[2]) != int(sys.argv[1]):
                    counter=counter+1  
                    # logger_time.info("Batch skipped:"+batch +":"+str(counter)+":App:"+ str(sys.argv[1]) + "::size:"+ str(len(scndetssys1))) 
                    continue
                
                batch=sys1[i][0]
                
                # if batch!= "BE1/AC.STMT.UPDATE" :
                #     continue     
                mintime=sys1[i][1]
                maxtime=sys1[i][2]
                scndetssys1=oracleDaosys1.fbatchRangeSearch(batch, processRow[14],mintime,maxtime,tablewildcard,tablewildcardallow)
                
                if scndetssys1 == None or len(scndetssys1) ==0:
                    counter=counter+1  
                    logger_info.info("oldsys Batch with zero rows ::"+batch)
                    continue;
                totalrowss1=totalrowss1+len(scndetssys1)
                logger_time.info("Batch processed:"+batch +":"+str(counter)+":App:"+ str(sys.argv[1]) + "::size:"+ str(len(scndetssys1))) 
                logger_info.info("oldsys Batch with  rows ::"+batch+"::"+str(len(scndetssys1))) 
                #scndetssys1 = sys1df.loc[(sys1df['UPDATETS']> datetime.strptime(mintime, '%Y-%m-%d %H:%M:%S.%f')) & (sys1df['UPDATETS']< datetime.strptime(maxtime, '%Y-%m-%d %H:%M:%S.%f')) & (sys2df['TNAME']!='UPDATING on F_LOCKING')] 
                for row in sys2 :
                    if row[0]== batch :                     
                        batchsys2=row[0]
                        mintimesys2=row[1]
                        maxtimesys2= row[2]
                        batchsys2=row[0]
                scndetssys2=oracleDaosys2.fbatchRangeSearch(batchsys2, processRow[21],mintimesys2,maxtimesys2,tablewildcard,tablewildcardallow)
                if  scndetssys2 == None :
                    logger_info.info("newsys Batch with zero rows ::"+batch)
                    counter=counter+1  
                    continue
                # compareDataForBatchOneCycle(dplistenDaoObj,diffTableDaoObj,batch,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct)
                counter=counter+1
                totalrowss2=totalrowss2+len(scndetssys2)
                 
               
                t = threading.Thread(target=compareDataForBatchOneCycle,args=(dplistenDaoObj,diffTableDaoObj,batch,scndetssys1,scndetssys2,taskid,runid,realtname,ignoretable,difftableStruct))
                t.start()                 
                threadrunner.append(t)
              
                logger_info.info("Batch processed Success ::  "+batch  + "   index of batch===>"+str(counter))
            
            except Exception:
                logger_debug.debug("exception "+ batch,exc_info=1)        
            # counter=counter+1  
        # dplistenDaoObj.updateRowCount(taskid,runid, totalrowss1,totalrowss2)          
        for threadm in threadrunner :
            threadm.join()    
                
        logger_info.info("Exiting Main Thread") 
        end = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        clearfile("cont.txt","")
        logger_info.info("end :")
        logger_info.info("Total time ===== :"+str(end-start))
        logger_info.info('done')   
    except Exception:
        logger_debug.debug("exception ",exc_info=1)              
    finally:
        dplistenDaoObj.closeprrowConnection(); 
        diffTableDaoObj.closeprrowConnection()      


# a loop to run the check every 2 seconds- as to lessen cpu usage
def sleepLoop():
    while 0 == 0:
        tableListener()
        logger_info.info("Listening ....**********----->")
        time.sleep(10.0)
os.system('cls')
logger_info.info('hi')
n = len(sys.argv)
print("Total arguments passed:", n)

# # Arguments passed
# print("\nName of Python script:", sys.argv[0])

print("\nArguments passed:", end = " ")
for i in range(1, n):
     print(sys.argv[i], end = " ")
# readcont=readfile("cont.txt")

# if readcont !=  "" :     
#     inputval = input('OLd Run Exixt Press Y to Continue where you left \n , N to Proceed Normal Flow ...\n') 
#     if inputval.upper() == "Y": 
#         tableListenercontinue(readcont)
sleepLoop()
  

logger_info.info('completed')
logger_info.info('done')
 