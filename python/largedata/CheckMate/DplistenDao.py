import logging
import os
import sys
import psycopg2
import CommonDao


class DplistenDao(CommonDao.CommonDao):

    def forceCommit(this):

        this.connection.coommit()

    def updateRowCount(this, taskid, runid, countsys1, countsys2):

        try:
            this.connect()
            statement = (
                "update DP_LISTEN_TABLE set ROWCOUNT='"
                + str(countsys1)
                + "',ROWCOUNTSYS2='"
                + str(countsys2)
                +"'  where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            this.cursor.execute(statement)
            this.connection.commit()
            this.logger_info.info("Status updated Succesful")

        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()




    def updateStatusdone(this, taskid, runid, status):

        try:
            this.connect()
            n=len(sys.argv)
            if n ==3 :
              arg2=sys.argv[2]
              statement = (
                "update DP_LISTEN_TABLE set UPDATETS=Current_timestamp, tcounter=COALESCE(tcounter,0)-1   where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            else: 
                statement = (
                "update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status='"
                + str(status)
                + "'  where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            print(statement)     
            this.cursor.execute(statement)
            if n ==3 :
               
                statement = (
                    "update DP_LISTEN_TABLE set   status='"
                    + str(status)
                    + "'  where  taskid='"
                    + str(taskid)
                    + "' and  runid='"
                    + str(runid)
                    + "' and  tcounter = '0'"
                )
                print(statement) 
                this.cursor.execute(statement)
            
            this.connection.commit()
            this.logger_info.info("Status updated Succesful")

        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()
            
    def updateStatus(this, taskid, runid, status):

        try:
            this.connect()
            n=len(sys.argv)
            if n ==3 :
              arg2=sys.argv[2]
              statement = (
                "update DP_LISTEN_TABLE set UPDATETS=Current_timestamp, tcounter=COALESCE(tcounter,0)+1   where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            else: 
                statement = (
                "update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status='"
                + str(status)
                + "'  where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            print(statement)   
            this.cursor.execute(statement)
            if n ==3 :
               
                statement = (
                    "update DP_LISTEN_TABLE set   status='"
                    + str(status)
                    + "'  where  taskid='"
                    + str(taskid)
                    + "' and  runid='"
                    + str(runid)
                    + "' and  tcounter = '"+arg2 +"'"
                )
                print(statement) 
                this.cursor.execute(statement)
            
            this.connection.commit()
            this.logger_info.info("Status updated Succesful")

        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()

    def updaterowproceesed(this, taskid, runid, count):

        try:

            statement = (
                " update dp_listen_table set rowsprocessed=rowsprocessed+ "+str(count)+ "  where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            this.prrowcursor.execute(statement)
           
        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))

    def updateProcessTime(
        this, taskid, runid, loadtime, batchtime, comaparetime, reporttime
    ):

        try:

            this.connect()
            statement = (
                " update dp_listen_table set  dataloadtime='"
                + str(loadtime)
                + "',batchtime='"
                + str(batchtime)
                + "',comparetime='"
                + str(comaparetime)
                + "',reportgentime='"
                + str(reporttime)
                + "'    where  taskid='"
                + str(taskid)
                + "' and  runid='"
                + str(runid)
                + "' "
            )
            this.cursor.execute(statement)

            this.connection.commit()
            this.logger_info.info("Status/time updated Succesful")
        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with Postgress+++++++", str(e))
        finally:
            this.closeConnection()
            
    def getRowForPrpcessingwithId(this ,taskid,runid):

        try:
            this.connect()
            statement="select * from DP_LISTEN_TABLE where  taskid='"+ taskid +"' and  runid='"+runid + "'order by insertts"
            this.cursor.execute(statement)
            results = this.cursor.fetchone()
            return results
        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()
 
    def getRowForPrpcessing(this ):

        try:
            this.connect()
            statement = (
                "select * from DP_LISTEN_TABLE where status='CREATED'  and (select count(status) from DP_LISTEN_TABLE where status='INPROGRESS"+sys.argv[1]+"' ) = 0 order by insertts"
            )
            this.cursor.execute(statement)
            results = this.cursor.fetchone()
            return results
        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()   
# logging.basicConfig(
#     level=logging.DEBUG,
#     format="[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ",
#     handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
# )
# logger = logging.getLogger("ReadFromDb")
# logger.setLevel(logging.DEBUG)
# logger_debug.debug(os.path.abspath(os.getcwd()))
# dplistenDaoObj = DplistenDao(
#     logger, "postgres", "postgres", "password", "localhost", 5432
# )
# dplistenDaoObj.updateProcessTime(2, 25, 33, 33, 33, 33)

# dplistenDaoObj.connect()
# dplistenDaoObj.updaterowproceesed(2, 25)
# dplistenDaoObj.closeConnection()
# dplistenDaoObj.updateStatus(2, 25, "bashawww")
# dplistenDaoObj.updateRowCount(2, 25, 455,666)
