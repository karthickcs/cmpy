import logging
import os
import psycopg2

import CommonDao

class DiffTableDao(CommonDao.CommonDao):


    
    def insertRecords(this,taskid,runid,differeceval,maintranid,comptranid,compscore,tid,sequence,otname,otype,odesc):
        
        
        try:
            json_mylist = str(differeceval).replace("'","\"")
            
            statement="INSERT INTO  DIFF_TABLE (TASKID,runid, INSERTTS, MAINTRANID, NEWTRANID, SEQUENCE, DIFFERENCE, COUNTVAL, THREADID ,oracletname,tval,ttype) VALUES ('"+str(taskid)+"','"+str(runid)+"', Current_timestamp, '"+str(maintranid)+"', '"+str(comptranid)+"', '"+str(sequence)+"', '"+str(json_mylist)+"', '"+str(compscore)+"', '"+str(tid)+"', '"+str(otname) +"', '"+str(otype)+"', '"+str(odesc) +"')"
            this.prrowcursor.execute(statement)    		
            #print (statement)
            
            
        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with Postgress+++++++",str( e))
        
            
            
  