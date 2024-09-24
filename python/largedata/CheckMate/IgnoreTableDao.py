import logging
import os
import psycopg2

import CommonDao

class IgnoreTableDao(CommonDao.CommonDao):


    
    def getIgnoreList(this,taskid ):
        sys =[]
        try:
            this.connect()
            statement = (
                "select distinct tableList from ignoretable where taskid in ('ALL','" + str(taskid) + "')"
            )
            this.cursor.execute(statement)
            remaining_rows = this.cursor.fetchall()
            for rec in remaining_rows:
                
                try:

                    
                    sys.append( rec[0])
                
                                    

                   
                except Exception:
                    this.logger.debug("error ") 
                # remaining_rows = cursor.fetchone()

            return sys
        except psycopg2.DatabaseError as e:
            print("There is a problem with postgress+++++++", str(e))
        finally:
            this.closeConnection()   
 