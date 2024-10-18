# import logging
# import os
# import traceback
# import oracledb
# import psycopg2

# import CommonOraDao


# class OracleDao(CommonOraDao.CommonOraDao):

#     def GetMetaData(this, tname):
#         cursor = None
#         con = None
#         Mainsys = {}
#         try:
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()             
#             statement = (
#                 "select recid,xmltype.getclobval(xmlrecord)  from  "
#                 + tname
#                 + "     order by recid "
#             )
#             cursor.execute(statement)
#             remaining_rows = cursor.fetchall()

#             if remaining_rows == None:
#                 this.logger_info.info("No rows")
#                 return
#             for rec in remaining_rows:
#                 sys = []
#                 try:

#                     sys.append(rec[0])
#                     sys.append(rec[1].read())

#                     Mainsys[rec[0]] = sys
#                 except Exception:
#                     this.logger_info.info("error ")
#                 # remaining_rows = cursor.fetchone()

#             return Mainsys
#         except Exception:
#             traceback.print_exc()
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()            

                
#     def readClobTriggerTable(this,tname,startts,endts,tablewildcard):
#         cursor = None
#         con = None
       
#         Mainsys = []
#         try:
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()             
#             ignore =  ""
#             tablewildcardArray = tablewildcard.split("\r\n")
            
#             for tabwild in tablewildcardArray:
#                 if tabwild.strip().upper() != "":
#                     ignore = ignore + "  and tname like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
#             orderby= " order by scnnum,updatets,tname"
#             statement="select TNAME,ROWIDVAL,SCNNUM, information,UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "  + ignore + "  " + orderby
#             this.logger_info.info(statement)
             
#             cursor.execute(statement) 
                    
#             #results = cursor.fetchone()
#             remaining_rows = cursor.fetchall()
#             this.logger_info.info('connected'+str(len(remaining_rows))) 
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             for rec in remaining_rows:
#                 sys =[]
#                 try:
#                     sys.append(rec[0])
#                     sys.append(rec[1])
#                     sys.append(rec[2])            
#                     sys.append ( rec[3].read())
#                     sys.append(rec[4])
#                     Mainsys.append(sys)
#                 except Exception:
#                     # this.logger_debug.debug("error clob") 
#                     err=1
#                 # remaining_rows = cursor.fetchone()
#             this.logger_info.info('connected'+str(len(Mainsys))) 
#             return Mainsys
#         except Exception:
#             traceback.print_exc()
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()                            

#     def countRows(this,tname,startts,endts):
#         cursor = None
#         con = None
       
#         try:
        
#             Mainsys=[]
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()              
#             statement="SELECT count(*) FROM "  + tname   + " where TNAME not like '%_BATCH%' and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "
#             this.logger_info.info(statement)
#             cursor.execute(statement)    		
            
            
#             remaining_rows = cursor.fetchone()
            
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return 0;
#             return  remaining_rows[0]
            
#         except Exception:
#             this.logger_debug.debug("exception ",exc_info=1)
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()            

#     def fbatchRangeSearch(this,tname,startts,endts,tablewildcard):
#         cursor = None
#         con = None
        
#         Mainsys = []
#         try:
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()             
#             ignore =  ""
#             tablewildcardArray = tablewildcard.split("\r\n")
#             for tabwild in tablewildcardArray:
#                 if tabwild.strip().upper() != "":
#                     ignore = ignore + "  and tname like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
#             orderby= " order by scnnum,updatets,tname"
#             statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where TNAME not like '%_BATCH%'  and updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6') "  + ignore + "  " + orderby
#             this.logger_info.info(statement)
            
#             cursor.execute(statement) 
             
#             remaining_rows = cursor.fetchall()
            
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             for rec in  remaining_rows:
#                 sys =[]
#                 try:
#                     sys.append(rec[0])
#                     sys.append(rec[1])
#                     sys.append(rec[2])            
#                     sys.append ( rec[3].read())
#                     sys.append(rec[4])
#                     Mainsys.append(sys)
#                 except Exception:
#                     this.logger_info.info("Error reading xml for: "+rec[0]+":"+rec[1]+":"+rec[2]) 
#                 # remaining_rows = cursor.fetchone()
#             this.logger_info.info('No of rows ::----->'+str(len(Mainsys))) 
#             return Mainsys
#         except Exception:
#             this.logger_debug.debug("exception ",exc_info=1)
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()             
            
    
#     def oracleGetRealTnameType(this ):
#         cursor = None
#         con = None
       
#         Mainsys = {}
#         try:
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor() 
            
            
            
#             statement = """  select a.recid,a.result,b.oracfname,c.recid||'-'||c.description
#             from (SELECT
#                 recid,            
#                extract(xmlrecord, '/row/c5/text()').getstringval() product,
#                 (CASE WHEN RECID LIKE '%PARAM%' THEN 'ST'   when  extract(xmlrecord, '/row/c5/text()').getstringval()='ST' Then 'ST' else 'TR' END) result
#             FROM
#                 f_pgm_file
#             WHERE
#                     extract(xmlrecord, '/row/c1/text()').getstringval() <> 'S'
#                 AND extract(xmlrecord, '/row/c1/text()').getstringval() <> 'M'
#                 AND extract(xmlrecord, '/row/c1/text()').getstringval() <> 'B'
#             ) a,(    
#             select   distinct (CASE WHEN INSTR(recid, '$') > 0
#         THEN  substr(recid, INSTR(recid,'.')+1, instr(recid, '$') - INSTR(recid,'.')-1)
#         ELSE  substr(recid, INSTR(recid,'.')+1)
#          END)   recid ,SUBSTR(ORCLFILENAME, INSTR(ORCLFILENAME,'_', 1, 1)+1)  oracfname from tafj_voc b where ORCLFILENAME is not null ) b,
#            (select recid,extract(xmlrecord, '/row/c1/text()').getstringval()  as description from F_EB_PRODUCT) c
#         where a.recid=b.recid  and
#         a.product=c.recid    """
    
#             cursor.execute(statement)    		
#             remaining_rows = cursor.fetchall()

#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             for rec in remaining_rows:
#                 sys =[]
#                 try:

#                     sys.append(rec[0])
#                     sys.append( rec[1])
                
#                     sys.append( rec[3])
                    

#                     Mainsys[rec[2]]=sys
#                 except Exception:
#                     this.logger_info.info("error ") 
#                 # remaining_rows = cursor.fetchone()

#             return Mainsys
#         except Exception:
#             traceback.print_exc()
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()                 
        


        
    
#     def readFromTriggerTable(this,tname,startts,endts,tablewildcard):
#         cursor = None
#         con = None
        
#         Mainsys = []
#         try:
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()             
#             ignore =  ""
#             tablewildcardArray = tablewildcard.split("\r\n")
#             for tabwild in tablewildcardArray:
#                 if tabwild.strip().upper() != "":
#                     ignore = ignore + "  and tname like '%"+ tabwild.replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            
       
#             orderby= " order by scnnum,updatets,tname"
#             statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "   + ignore + "  " + orderby
#             this.logger_info.info(statement)
             
#             cursor.execute(statement) 
              
#             remaining_rows = cursor.fetchone()
            
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             while remaining_rows:
#                 sys =[]
#                 try:
#                     sys.append(remaining_rows[0])
#                     sys.append(remaining_rows[1])
#                     sys.append(remaining_rows[2])            
#                     sys.append ( remaining_rows[3].read())
#                     sys.append(remaining_rows[4])
#                     Mainsys.append(sys)
#                 except Exception:
#                     this.logger_info.info("error ") 
#                 remaining_rows = cursor.fetchone()
#             this.logger_info.info('connected'+str(len(Mainsys))) 
#             return Mainsys
#         except Exception:
#             traceback.print_exc()
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()            
            
            
#     def readFromTriggerTableCont(this,tname,startts,endts,oldarr):
#         cursor = None
#         con = None

#         try:
        
#             Mainsys=[]
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor()            
#             statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%'"  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +"  GROUP BY ROWIDVAL order by ROWIDVAL "
#             cursor.execute(statement)    		
#             #results = cursor.fetchone()
            
#             remaining_rows = cursor.fetchall()
            
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             for rec in remaining_rows:
#                 sys =[]
#                 try:
#                     if rec[0] in oldarr:
#                         this.logger_info.info("Already Processed"+ rec[0])
#                     else:
#                         this.logger_info.info(rec[0])
#                         sys.append(rec[0])
#                         sys.append(rec[1])
#                         sys.append(rec[2])                  
#                         Mainsys.append(sys)
#                 except Exception:
#                     this.logger_info.info("error ") 
#                 # remaining_rows = cursor.fetchone()
#             this.logger_info.info("Total records Batch Processing---->"+str(len(Mainsys)))
#             return Mainsys
#         except Exception:
#             this.logger_debug.debug("exception ",exc_info=1)
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()                    
    
#     def fbatchMinMax(this,tname,startts,endts,oldarr):
        
#         cursor = None
#         con = None
        
#         try:
            
#             Mainsys=[]
#             con=oracledb.connect(
#             user=this.username,
#             password=this.password,
#             dsn=this.jdbcurl)
     
#             cursor = con.cursor() 
#             statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%' "  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +" GROUP BY ROWIDVAL"
#             cursor.execute(statement)    		
             
            
#             remaining_rows = cursor.fetchall()
            
#             if remaining_rows == None :
#                 this.logger_info.info('No rows')
#                 return
#             for rec in remaining_rows:
#                 sys =[]
#                 try:
#                     if rec[0] in oldarr:
#                         this.logger_info.info("Already Processed"+ rec[0])
#                     else:
#                         sys.append(rec[0])
#                         sys.append(rec[1])
#                         sys.append(rec[2])                  
#                         Mainsys.append(sys)
#                 except Exception:
#                     this.logger_info.info("error ") 
#                 # remaining_rows = cursor.fetchone()
            
#             return Mainsys
#         except Exception:
#             this.logger_debug.debug("exception ",exc_info=1)
#         finally:
#             if cursor:
#                 cursor.close()
#             if con:
#                 con.close()
            
                
# # logging.basicConfig(
# #     level=logging.DEBUG,
# #     format="[%(asctime)s:%(lineno)s - %(funcName)20s() ] %(message)s ",
# #     handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
# # )
# # logger = logging.getLogger("ReadFromDb")
# # logger.setLevel(logging.DEBUG)
# # this.logger_debug.debug(os.path.abspath(os.getcwd()))
# # oracleDao = OracleDao(logger, "newsys", "password", "localhost:1521/XEPDB1")
