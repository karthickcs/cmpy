from collections import defaultdict
import json
import logging
import os
import sys
import threading
import dictdiffer
import pandas as pd
import psycopg2
import itertools
import csv 
from lxml import etree,objectify

import DiffTableDao
import DplistenDao
import XmlProcessor
class FileProcessor:
    def __init__(this, logger_info,logger_debug ):
        this.logger_info = logger_info
        this.logger_debug = logger_debug
        
        
                
    def processFiles(this, srcfolder, targetfolder,dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao, taskid,runid):
        this.logger_info.info(srcfolder)
        this.logger_info.info(targetfolder)
        try:
            srcfolderDir = os.listdir(srcfolder)
            targetfolderDir = os.listdir(targetfolder)
            realfilessrc=this.getonlycsv(srcfolderDir);
            realfilestar=this.getonlycsv(targetfolderDir);
            allfiledatasrc=this.getAlldata(realfilessrc,srcfolder)
            allfiledatatarget=this.getAlldata(realfilestar,targetfolder)
            
            keysListSrc = list(allfiledatasrc.keys())
            for key in keysListSrc:
                srcdata=allfiledatasrc[key]
                if key in allfiledatatarget :
                    tardata=allfiledatatarget[key]
                    this.logger_info.info("Processing File "+ key)
                    this.compareDataForOnlinethread(dplistenDaoObj,diffTableDaoObj ,'Fileprocess',srcdata,tardata, taskid , runid)
                else:
                    this.logger_info.info("Type missing in target folder-===="+key)  
        except Exception:
             this.logger_debug.debug("exception ",exc_info=1)   
                     
        this.logger_info.info("Processing completed ")
        return
    
    
    def getAlldata(this,realfilesdata,folderDir):
        listoffiles={}
        for rec in realfilesdata:
            filetypedict={}
            realfname=""
            for filevv  in rec:
                try:
                    realfname=filevv[:filevv.rfind("_")] 
                    filetypedict=this.readcsvdata(folderDir+"\\"+ filevv ,filevv,filetypedict ) 
                except Exception:
                    this.logger_debug.debug("exception ",exc_info=1)     
                 
            listoffiles[realfname]=filetypedict
                

        
        return listoffiles
    
    def getonlycsv(this,folderval):
        filelist=[]
        for fileval in folderval:
            if fileval.find(".csv") > -1:
                filelist.append(fileval)
        keyfunc = lambda filename: filename[:filename.rfind("_")]
        grouper = itertools.groupby(filelist, keyfunc)
        result = [list(filelist) for _, filelist in grouper]

                
        return list(result);


               
    def readcsvdata(this,fullfname,filevv,filetypedict):
        totalerr=""
         
        i=0
        indexval=0
        datadictcolname=[]
        try:
             
            with open(fullfname, 'r', encoding='utf_16', errors='replace') as file:    
                for line in file:
                    
                    if i==0 :
                         datadictcolname=line.split("~")
                         indexval=datadictcolname.index("@ID")
                         i=i+1
                         continue
                     
                     
                    filetypedict[line.split("~")[indexval]]=this.getdatafromcsv(line,datadictcolname)
                    i=i+1
                    
        except Exception:
             this.logger_debug.debug("exception ",exc_info=1) 
        print(totalerr)
        return filetypedict
    
    
    def getdatafromcsv(this,text,datadictcolname):
        datadict ={}
        try:
            
            row = text.split("~")
            
            i=0
            for value in row:
                 
                datadict[datadictcolname[i].replace('@','').replace('\n','')]=value
                i=i+1 
        except:
            this.logger_debug.debug("exception ",exc_info=1)   
            
         
        return datadict
    
    def getxmlfromclobdata(this,text,datadictcolname):
        root = etree.Element("root")
        try:
            
            row = text.split("~")
            root = etree.Element("root")
            child = etree.SubElement(root, "row")
            i=0
            for value in row:
                etree.SubElement(child, datadictcolname[i].replace('@','').replace('\n','')).text = value
                i=i+1
        except:
            this.logger_debug.debug("exception ",exc_info=1)   
            
        #print (etree.tostring(root, encoding="unicode"))
        return etree.tostring(root, encoding='utf-8')

     
    # def compareDataForFile(this,dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid,scndetssys1,scndetssys2,taskid,runid ):
        
    #     try:
    #         xmlProcessor = XmlProcessor.XmlProcessor(this.logger_info,this.logger_debug)
          
    #         intriger=1000       
    #         inrval=0
    #         inrr=int(len(scndetssys1)/intriger)
    #         inthreadrunner=[]
    #         dplistenDaoObj.connectprrow()
    #         diffTableDaoObj.connectprrow()
    #         inrr+=1
    #         for i in range(inrr):
    #             t = threading.Thread(target=compareDataForOnlinethread,args=(this,dplistenDaoObj,diffTableDaoObj,tid+str(i),inrval,inrval+intriger, scndetssys1, scndetssys2, taskid,runid))
            
    #             t.start()
    #             this.logger_info.info("start Batch  Thread of "+tid +"-->"+str(i)) 
    #             inthreadrunner.append(t)
    #             inrval=inrval+intriger
                
    #         for threadm in inthreadrunner :
    #             threadm.join()    
            
    #         this.logger_info.info("Exiting Batch  Thread of  Thread of "+tid) 
            
            
        
        # except Exception:
        #     this.logger_debug.debug("exception ",exc_info=1)
        # finally:
        #     dplistenDaoObj.closeprrowConnection(); 
        #     diffTableDaoObj.closeprrowConnection()   
            
             
    
    
    
                                   
    def compareDataForOnlinethread(this,dplistenDaoObj:DplistenDao.DplistenDao,diffTableDaoObj:DiffTableDao.DiffTableDao,tid ,tab1,tab2 ,taskid,runid):
        
        try:  
        
            Masterlist=[]
            i=0 
            dplistenDaoObj.connectprrow()
            diffTableDaoObj.connectprrow()
            keysListtab1 = list(tab1.keys())
            for kvalmaster in keysListtab1 :
                i=i+1             
                sublist=[]
               
                master=tab1[kvalmaster ]
                
                follower=tab2[kvalmaster]
                difflist = list(dictdiffer.diff(master, follower))
                difflistlength=0
                for t in difflist:
                    difflistlength=difflistlength + len(t[2])
                    
                
                                    
                sublist.append(difflist)
                sublist.append(kvalmaster )
                sublist.append(kvalmaster)
                sublist.append(difflistlength)
                sublist.append("0")
                Masterlist.append(sublist)
                if difflistlength > 0:
                
                    tnamevall=kvalmaster 
                    substring_start = tnamevall[:tnamevall.find('_')+1]
                    substring_end = tnamevall[tnamevall.find('_')+1:]
                    # if substring_end in realtname:
                    #     realtnameResult=realtname[substring_end]
                    # else:
                    #     realtnameResult=substring_end    
                    # oname=""
                    # otype=""
                    # if realtnameResult :
                    #     oname=substring_start+realtnameResult[0]
                    #     otype=realtnameResult[1]
                    #     odesc=realtnameResult[2]               
                    diffTableDaoObj.insertRecords(taskid,runid,difflist,kvalmaster,kvalmaster,difflistlength,tid ,i,tnamevall,'ST',"Common")
                    diffTableDaoObj.prrowconnection.commit()
                dplistenDaoObj.updaterowproceesed(taskid,runid)   
                
                dplistenDaoObj.prrowconnection.commit()
        except Exception:
            this.logger_debug.debug("exception ",exc_info=1)
        finally:
            dplistenDaoObj.closeprrowConnection()
            diffTableDaoObj.closeprrowConnection()       
        
        df = pd.DataFrame(Masterlist)
        
        
        df.to_csv('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', mode='a', header=False) 
        
        
        
        
 
       