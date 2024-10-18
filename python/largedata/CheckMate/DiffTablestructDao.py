import logging
import os
import psycopg2

import CommonDao

class DiffTablestructDao(CommonDao.CommonDao):


    def deleteoldRecords(this, taskid):

        try:
            this.logger_info.info("Delete old records")
            this.connect()
            
            statement = (
                "delete  from  Diff_Table_struct where  taskid='" + str(taskid) + "'   "
            )
            this.cursor.execute(statement)
            this.connection.commit()

        except psycopg2.DatabaseError as e:
            this.logger_debug.debug("There is a problem with Postgress", str(e))
        finally:
            this.closeConnection()

    def insertRecords(
        this, taskid, runid, tname, oldjson, newjson, difference
    ):

        try:
            oldjson_str = str(oldjson).replace("'", '"')
            newjson_str = str(newjson).replace("'", '"')
            difference_mylist = str(difference).replace("'", '"')

            statement = (
                "INSERT INTO  Diff_Table_struct (TASKID,runid, TNAME, TSTRUCTOLDSYS,TSTRUCTNEWSYS, DIFFERENCE ) VALUES ('"
                + str(taskid)
                + "','"
                + str(runid)
                + "','"
                + str(tname)
                + "', '"
                + str(oldjson_str)
                + "', '"
                + str(newjson_str)
                + "', '"
                + str(difference_mylist)
                + "')"
            )
            this.cursor.execute(statement)
            this.connection.commit()

        except psycopg2.DatabaseError as e:
            print("There is a problem with Postgress+++++++", str(e))


    def getTableStructure(this,taskid ):
        Mainsys={}
        try:
            this.connect()
            statement = (
                "select tname,tstructoldsys,tstructnewsys from diff_table_struct where taskid='" + str(taskid) + "'"
            )
            this.cursor.execute(statement)
            remaining_rows = this.cursor.fetchall()
            for rec in remaining_rows:
                sys =[]
                try:

                    
                    sys.append( rec[1])
                
                    sys.append( rec[2])                   

                    Mainsys[rec[0]]=sys
                except Exception:
                    this.logger_debug.debug("error ") 
                # remaining_rows = cursor.fetchone()

            return Mainsys
        except psycopg2.DatabaseError as e:
            print("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()   
 