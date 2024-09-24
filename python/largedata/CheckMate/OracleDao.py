import logging
import os
import traceback
import oracledb
import psycopg2

import CommonOraDao


class OracleDao(CommonOraDao.CommonOraDao):

    def GetMetaData(this, tname):

        Mainsys = {}
        try:
            this.connect()
            statement = (
                "select recid,xmltype.getclobval(xmlrecord)  from  "
                + tname
                + "     order by recid "
            )
            this.cursor.execute(statement)
            remaining_rows = this.cursor.fetchone()

            if remaining_rows == None:
                this.logger.debug("No rows")
                return
            while (remaining_rows):
                sys = []
                try:

                    sys.append(remaining_rows[0])
                    sys.append(remaining_rows[1].read())

                    Mainsys[remaining_rows[0]] = sys
                except Exception:
                    this.logger.debug("error ")
                remaining_rows = this.cursor.fetchone()

            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            this.closeConnection()

                
    def readClobTriggerTable(this,tname,startts,endts,tablewildcard):
       
        Mainsys = []
        try:
            
            ignore =  ""
            tablewildcardArray = tablewildcard.split("\r\n")
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    ignore = ignore + "  and tname not like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
            orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM, information,UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "  + ignore + "  " + orderby
            this.logger.debug(statement)
            this.connect() 
            this.cursor.execute(statement) 
                    
            #results = cursor.fetchone()
            remaining_rows = this.cursor.fetchone()
            this.logger.debug('connected'+str(len(remaining_rows))) 
            if remaining_rows == None :
                this.logger.debug('No rows')
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
                    # this.logger.debug("error clob") 
                    err=1
                remaining_rows = this.cursor.fetchone()
            this.logger.debug('connected'+str(len(Mainsys))) 
            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            this.closeConnection()                

    def countRows(this,tname,startts,endts):
        
        try:
        
            Mainsys=[]
            this.connect() 
            statement="SELECT count(*) FROM "  + tname   + " where TNAME not like '%_BATCH%' and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "
            this.cursor.execute(statement)    		
            
            
            remaining_rows = this.cursor.fetchone()
            
            if remaining_rows == None :
                this.logger.debug('No rows')
                return 0;
            return  remaining_rows[0]
            
        except Exception:
            this.logger.error("exception ",exc_info=1)
        finally:
            this.closeConnection()

    def fbatchRangeSearch(this,tname,startts,endts,tablewildcard):
        
        Mainsys = []
        try:
            this.connect()
            ignore =  ""
            tablewildcardArray = tablewildcard.split("\r\n")
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    ignore = ignore + "  and tname not like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
            orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6') "  + ignore + "  " + orderby
            this.logger.debug(statement)
            
            this.cursor.execute(statement) 
             
            remaining_rows = this.cursor.fetchone()
            
            if remaining_rows == None :
                this.logger.debug('No rows')
                return
            while remaining_rows:
                sys =[]
                try:
                    sys.append(remaining_rows[0])
                    sys.append(remaining_rows[1])
                    sys.append(remaining_rows[2])            
                    sys.append ( remaining_rows[3].read())
                    sys.append(remaining_rows[4])
                    Mainsys.append(sys)
                except Exception:
                    this.logger.debug("error clob") 
                remaining_rows = this.cursor.fetchone()
            this.logger.debug('No of rows ::----->'+str(len(Mainsys))) 
            return Mainsys
        except Exception:
            this.logger.error("exception ",exc_info=1)
        finally:
            this.closeConnection() 
            
    
    def oracleGetRealTnameType(this ):
       
        Mainsys = {}
        try:

            
            this.connect()
            
            statement = """  select a.recid,a.result,b.oracfname,c.recid||'-'||c.description
            from (SELECT
                recid,            
               extract(xmlrecord, '/row/c5/text()').getstringval() product,
                (CASE WHEN RECID LIKE '%PARAM%' THEN 'ST'   when  extract(xmlrecord, '/row/c5/text()').getstringval()='ST' Then 'ST' else 'TR' END) result
            FROM
                f_pgm_file
            WHERE
                    extract(xmlrecord, '/row/c1/text()').getstringval() <> 'S'
                AND extract(xmlrecord, '/row/c1/text()').getstringval() <> 'M'
                AND extract(xmlrecord, '/row/c1/text()').getstringval() <> 'B'
            ) a,(    
            select   distinct (CASE WHEN INSTR(recid, '$') > 0
        THEN  substr(recid, INSTR(recid,'.')+1, instr(recid, '$') - INSTR(recid,'.')-1)
        ELSE  substr(recid, INSTR(recid,'.')+1)
         END)   recid ,SUBSTR(ORCLFILENAME, INSTR(ORCLFILENAME,'_', 1, 1)+1)  oracfname from tafj_voc b where ORCLFILENAME is not null ) b,
           (select recid,extract(xmlrecord, '/row/c1/text()').getstringval()  as description from F_EB_PRODUCT) c
        where a.recid=b.recid  and
        a.product=c.recid    """
    
            this.cursor.execute(statement)    		
            remaining_rows = this.cursor.fetchall()

            if remaining_rows == None :
                this.logger.debug('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:

                    sys.append(rec[0])
                    sys.append( rec[1])
                
                    sys.append( rec[3])
                    

                    Mainsys[rec[2]]=sys
                except Exception:
                    this.logger.debug("error ") 
                # remaining_rows = cursor.fetchone()

            return Mainsys
        except Exception:
            traceback.print_exc()
        


        
    
    def readFromTriggerTable(this,tname,startts,endts,tablewildcard):
        
        Mainsys = []
        try:
            this.connect()
            ignore =  ""
            tablewildcardArray = tablewildcard.split("\r\n")
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    ignore = ignore + "  and tname not like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
            orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "   + ignore + "  " + orderby
            this.logger.debug(statement)
             
            this.cursor.execute(statement) 
              
            remaining_rows = this.cursor.fetchone()
            
            if remaining_rows == None :
                this.logger.debug('No rows')
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
                    this.logger.debug("error ") 
                remaining_rows = this.cursor.fetchone()
            this.logger.debug('connected'+str(len(Mainsys))) 
            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            this.closeConnection()
            
            
    def readFromTriggerTableCont(this,tname,startts,endts,oldarr):
    
        try:
        
            Mainsys=[]
            this.connect()
            statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%'"  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +"  GROUP BY ROWIDVAL"
            this.cursor.execute(statement)    		
            #results = cursor.fetchone()
            
            remaining_rows = this.cursor.fetchall()
            
            if remaining_rows == None :
                this.logger.debug('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:
                    if remaining_rows[0] in oldarr:
                        this.logger.debug("Already Processed"+ remaining_rows[0])
                    else:
                        sys.append(rec[0])
                        sys.append(rec[1])
                        sys.append(rec[2])                  
                        Mainsys.append(sys)
                except Exception:
                    this.logger.debug("error ") 
                # remaining_rows = this.cursor.fetchone()
            
            return Mainsys
        except Exception:
            this.logger.error("exception ",exc_info=1)
        finally:
            this.closeConnection()        
    
    def fbatchMinMax(this,tname,startts,endts,oldarr):
        cursor = None
        con = None
        
        try:
            
            Mainsys=[]
            this.connect()
            statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%' "  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +" GROUP BY ROWIDVAL"
            this.cursor.execute(statement)    		
             
            
            remaining_rows = this.cursor.fetchall()
            
            if remaining_rows == None :
                this.logger.debug('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:
                    if remaining_rows[0] in oldarr:
                        this.logger.debug("Already Processed"+ remaining_rows[0])
                    else:
                        sys.append(rec[0])
                        sys.append(rec[1])
                        sys.append(rec[2])                  
                        Mainsys.append(sys)
                except Exception:
                    this.logger.debug("error ") 
                # remaining_rows = cursor.fetchone()
            
            return Mainsys
        except Exception:
            this.logger.error("exception ",exc_info=1)
        finally:
            this.closeConnection()
                
# logging.basicConfig(
#     level=logging.DEBUG,
#     format="[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ",
#     handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
# )
# logger = logging.getLogger("ReadFromDb")
# logger.setLevel(logging.DEBUG)
# logger.debug(os.path.abspath(os.getcwd()))
# oracleDao = OracleDao(logger, "newsys", "password", "localhost:1521/XEPDB1")
