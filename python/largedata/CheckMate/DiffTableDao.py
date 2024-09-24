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
            this.logger.debug("There is a problem with Postgress+++++++",str( e))
        
            
            
 
# logging.basicConfig(
#     level=logging.DEBUG,
#     format="[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ",
#     handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
# )
# logger = logging.getLogger("ReadFromDb")
# logger.setLevel(logging.DEBUG)
# logger.debug(os.path.abspath(os.getcwd()))
# DiffTableObj = DiffTableDao(
#     logger, "postgres", "postgres", "password", "localhost", 5432
# )
 

# DiffTableObj.connect()
# DiffTableObj.insertRecords(2,25,'test','2222','333333','33','3','1')
# DiffTableObj.closeConnection()

