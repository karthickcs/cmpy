import logging
import os
import traceback
import oracledb
import psycopg2

import CommonOraDao


class OracleDao(CommonOraDao.CommonOraDao):

    def GetMetaData(this, tname):
        cursor = None
        con = None
        Mainsys = {}
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor()             
            statement = (
                "select recid,xmltype.getclobval(xmlrecord)  from  "
                + tname
                + "     order by recid "
            )
            cursor.execute(statement)
            remaining_rows = cursor.fetchall()

            if remaining_rows == None:
                this.logger_info.info("No rows")
                return
            for rec in remaining_rows:
                sys = []
                try:

                    sys.append(rec[0])
                    sys.append(rec[1].read())

                    Mainsys[rec[0]] = sys
                except Exception:
                    this.logger_info.info("error ")
                # remaining_rows = cursor.fetchone()

            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()            

                
    def readClobTriggerTable(this,tname,startts,endts,tablewildcard,tablewildcardallow):
        cursor = None
        con = None
       
        Mainsys = []
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor()     
            if tablewildcardallow == 'SPEED':
                ignore = this.getimptables(tname ) 
            else:                            
                ignore =  this.buildTablewildcardscnunmin(tablewildcard,tablewildcardallow,tname,startts,endts)
                     
            
          
       
            # orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM, information,UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "  + ignore + "  "  
            this.logger_info.info(statement)
             
            cursor.execute(statement) 
                    
            #results = cursor.fetchone()
            remaining_rows = cursor.fetchall()
            this.logger_info.info('connected'+str(len(remaining_rows))) 
            if remaining_rows == None :
                this.logger_info.info('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:
                    sys.append(rec[0])
                    sys.append(rec[1])
                    sys.append(rec[2])            
                    sys.append ( rec[3].read())
                    sys.append(rec[4])
                    Mainsys.append(sys)
                except Exception:
                    # this.logger_debug.debug("error clob") 
                    err=1
                # remaining_rows = cursor.fetchone()
            this.logger_info.info('connected'+str(len(Mainsys))) 
            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()                            

    def countRows(this,tname,startts,endts,tablewildcard,tablewildcardallow):
        cursor = None
        con = None
       
        try:
        
            Mainsys=[]
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
            if tablewildcardallow == 'SPEED':
                ignore = this.getimptables(tname ) 
            else:                            
                ignore =  this.buildTablewildcardscnunmin(tablewildcard,tablewildcardallow,tname,startts,endts)
            
            cursor = con.cursor()              
            statement="SELECT count(*) FROM "  + tname   + " where  updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') " + ignore
            this.logger_info.info(statement)
            cursor.execute(statement)    		
            
            
            remaining_rows = cursor.fetchone()
            
            if remaining_rows == None :
                this.logger_info.info('No rows')
                return 0;
            return  remaining_rows[0]
            
        except Exception:
            this.logger_debug.debug("exception ",exc_info=1)
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()            

    def fbatchRangeSearch(this,batch,tname,startts,endts,tablewildcard,tablewildcardallow):
        cursor = None
        con = None
        
        Mainsys = []
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor() 
            if tablewildcardallow == 'SPEED':
                ignore = this.getimptables(tname ) 
            else:                            
                ignore =  this.buildTablewildcardscnunminSpecial(tablewildcard,tablewildcardallow,tname,startts,endts)
             
            
       
            # orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where   updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6') " + this.buildTablewildcardIgnoreTables() + ignore    
            this.logger_info.info(batch+":"+statement)
            countstatement="select count(*) from "  + tname   + " where   updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6') "  + ignore    
            cursor.execute(countstatement) 
            rows = cursor.fetchone()
            
            
            
            if rows[0] > 2999 : 
                cursor.execute(statement)  
                remaining_rows = cursor.fetchone()
                
                if remaining_rows == None :
                    this.logger_info.info('No rows')
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
                        this.logger_info.info("Error reading xml for: "+remaining_rows[0]+":"+remaining_rows[1]+":"+remaining_rows[2]) 
                    remaining_rows = cursor.fetchone()
                this.logger_info.info('No of rows ::----->'+str(len(Mainsys))) 
                return Mainsys
            elif rows[0] > 0:
                cursor.execute(statement) 
                remaining_rows = cursor.fetchall()
                
                if remaining_rows == None :
                    this.logger_info.info('No rows')
                    return
                for rec in  remaining_rows:
                    sys =[]
                    try:
                        sys.append(rec[0])
                        sys.append(rec[1])
                        sys.append(rec[2])            
                        sys.append (rec[3].read())
                        sys.append(rec[4])
                        Mainsys.append(sys)
                    except Exception:
                        this.logger_info.info("Error reading xml for: "+rec[0]+":"+rec[1]+":"+rec[2]) 
                     
                this.logger_info.info('No of rows ::----->'+str(len(Mainsys))) 
                return Mainsys
                
            
        except Exception:
            this.logger_debug.debug("exception ",exc_info=1)
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()             
    
    
    
    def oracleGetRealTnameType(this ):
        cursor = None
        con = None
       
        Mainsys = {}
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor() 
            
            
            
            statement = """  select   b.recid,a.result,b.oracfname,c.recid||'-'||c.description
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
         END)   recid ,SUBSTR(ORCLFILENAME, INSTR(ORCLFILENAME,'_', 1, 1)+1)  oracfname  from tafj_voc  where ORCLFILENAME is not null ) b,
           (select recid,extract(xmlrecord, '/row/c1/text()').getstringval()  as description from F_EB_PRODUCT) c
        where a.recid=b.recid  and
        a.product=c.recid    """
    
            cursor.execute(statement)    		
            remaining_rows = cursor.fetchall()

            if remaining_rows == None :
                this.logger_info.info('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:

                    sys.append(rec[0])
                    sys.append( rec[1])
                
                    sys.append( rec[3])
                    

                    Mainsys[rec[2]]=sys
                except Exception:
                    this.logger_info.info("error ") 
                # remaining_rows = cursor.fetchone()

            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()                 
        

    def oracleGettablefrompgm(this, tname ):
        cursor = None
        con = None
       
        Mainsys = {}
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor() 
            
            
            
            statement = """  select b.tname   from 
        (
        SELECT
        REPLACE( recid,'.','_')  tname
        FROM
            f_pgm_file
        WHERE
            extract(xmlrecord, '/row/c1/text()').getstringval() = 'H') a,
                        
            (select  distinct SUBSTR(tname, INSTR(tname,'_', 1, 1)+1)   tname from  """
            
            
              
            statement2 = """ ) b
            where a.tname=b.tname
            AND ( b.tname NOT LIKE '%BATCH%' ESCAPE '\\'
                AND b.tname NOT LIKE '%PROTOCOL%' ESCAPE '\\'
                AND b.tname NOT LIKE '%EB\\_EOD\\_ERROR%' ESCAPE '\\'
                AND b.tname NOT LIKE '%TAFJ%' ESCAPE '\\'
                AND b.tname NOT LIKE '%LOCK%' ESCAPE '\\'
                AND b.tname NOT LIKE '%OS\\_%' ESCAPE '\\' )        
        """
            fstatement = statement + tname + statement2
            fstatement = fstatement.replace('\n', '')
            cursor.execute(fstatement)    		
            remaining_rows = cursor.fetchall()

            if remaining_rows == None :
                this.logger_info.info('No rows')
                return
            
                # remaining_rows = cursor.fetchone()

            return remaining_rows
        except Exception:
            traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()    
        
    
    def readFromTriggerTable(this,tname,startts,endts,tablewildcard,tablewildcardallow):
        cursor = None
        con = None
        
        Mainsys = []
        try:
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor()             
            # ignore = this.getimptables(tablewildcard,tablewildcardallow,tname,startts,endts) 
            if tablewildcardallow == 'SPEED':
                ignore = this.getimptables(tname ) 
            else:                            
                ignore =  this.buildTablewildcardscnunmin(tablewildcard,tablewildcardallow,tname,startts,endts)
       
            # orderby= " order by scnnum,updatets,tname"
            statement="select TNAME,ROWIDVAL,SCNNUM,xmltype.getclobval(information),UPDATETS from "  + tname   + " where updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI') "   + ignore + "  "  
            this.logger_info.info(statement)
             
            cursor.execute(statement) 
              
            remaining_rows = cursor.fetchone()
            
            if remaining_rows == None :
                this.logger_info.info('No rows')
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
                    this.logger_info.info("error ") 
                remaining_rows = cursor.fetchone()
            this.logger_info.info('connected'+str(len(Mainsys))) 
            return Mainsys
        except Exception:
            traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()            
            
            
    def readFromTriggerTableCont(this,tname,startts,endts,oldarr):
        cursor = None
        con = None

        try:
        
            Mainsys=[]
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor()            
            statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' and xmltype.getclobval(information)    LIKE '%<c1>%' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%'"  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +"  GROUP BY ROWIDVAL order by ROWIDVAL "
            cursor.execute(statement)    		
            #results = cursor.fetchone()
            
            remaining_rows = cursor.fetchall()
            
            if remaining_rows == None :
                this.logger_info.info('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:
                    if rec[0] in oldarr:
                        this.logger_info.info("Already Processed"+ rec[0])
                    else:
                        this.logger_info.info(rec[0])
                        sys.append(rec[0])
                        sys.append(rec[1])
                        sys.append(rec[2])                  
                        Mainsys.append(sys)
                except Exception:
                    this.logger_info.info("error ") 
                # remaining_rows = cursor.fetchone()
            this.logger_info.info("Total records Batch Processing---->"+str(len(Mainsys)))
            return Mainsys
        except Exception:
            this.logger_debug.debug("exception ",exc_info=1)
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()                    
    
    def fbatchMinMax(this,tname,startts,endts,oldarr):
        
        cursor = None
        con = None
        
        try:
            
            Mainsys=[]
            con=oracledb.connect(
            user=this.username,
            password=this.password,
            dsn=this.jdbcurl)
     
            cursor = con.cursor() 
            statement="SELECT ROWIDVAL,MIN(UPDATETS) MIN ,MAX(UPDATETS) FROM "  + tname   + " WHERE TNAME ='UPDATING on F_BATCH' AND xmltype.getclobval(information) NOT  LIKE '%<c3>0</c3>%' "  +  " and updatets > TO_TIMESTAMP('"+startts + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+endts + "','YYYY-mm-dd\"T\"HH24:MI')  "  +" GROUP BY ROWIDVAL"
            cursor.execute(statement)    		
             
            
            remaining_rows = cursor.fetchall()
            
            if remaining_rows == None :
                this.logger_info.info('No rows')
                return
            for rec in remaining_rows:
                sys =[]
                try:
                    if rec[0] in oldarr:
                        this.logger_info.info("Already Processed"+ rec[0])
                    else:
                        sys.append(rec[0])
                        sys.append(rec[1])
                        sys.append(rec[2])                  
                        Mainsys.append(sys)
                except Exception:
                    this.logger_info.info("error ") 
                # remaining_rows = cursor.fetchone()
            
            return Mainsys
        except Exception:
            this.logger_debug.debug("exception ",exc_info=1)
        finally:
            if cursor:
                cursor.close()
            if con:
                con.close()
            
    def buildTablewildcard(this,tablewildcard,tablewildcardallow):
        ignore =  ""
        tablewildcardArray = tablewildcard.split("\r\n")
        if tablewildcardallow == 'LIKE' :
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and ( "
                        ignore = ignore + "   tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " ) "    
        elif  tablewildcardallow == 'NOTLIKE' : 
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and ( "
                        ignore = ignore + "   tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " ) "    
        return ignore  
    
    
    def buildTablewildcardscnunmin(this,tablewildcard,tablewildcardallow,tname,startts,endts):
        ignore =  ""
        tablewildcardArray = tablewildcard.split("\r\n")
        if tablewildcardallow == 'LIKE' :
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and  scnnum in ( select scnnum from  " + tname +" where "
                        ignore = ignore + " (  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " ) and  updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd\"T\"HH24:MI') )"    
        elif tablewildcardallow == 'NOTLIKE' :        
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and  scnnum in ( select scnnum from  " + tname +" where "
                        ignore = ignore + "   tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " )  updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd\"T\"HH24:MI')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd\"T\"HH24:MI')  "        
        
        return ignore            
 
    
    
    def buildTablewildcardscnunminSpecial(this,tablewildcard,tablewildcardallow,tname,startts,endts):
        ignore =  ""
        tablewildcardArray = tablewildcard.split("\r\n")
        if tablewildcardallow == 'LIKE' :
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and   scnnum in ( select scnnum from  " + tname +" where "
                        ignore = ignore + " (  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " ) and  updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  )"    
        elif tablewildcardallow == 'NOTLIKE' :        
            counter=1
            for tabwild in tablewildcardArray:
                if tabwild.strip().upper() != "":
                    if counter==1 :
                        counter=counter+1
                        ignore= " and   scnnum in ( select scnnum from  " + tname +" where "
                        ignore = ignore + "(   tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                        continue
                    ignore = ignore + " or  tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
            if ignore!= "":
                ignore= ignore+ " ) and  updatets > TO_TIMESTAMP('"+str(startts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  and updatets < TO_TIMESTAMP('"+str(endts) + "','YYYY-mm-dd HH24:MI:SS.FF6')  )"        
        
        return ignore    
    
    
    
    
    def buildTablewildcardIgnoreTables(this):
        ignore =  ""
          
        tablewildcardArray = ["F_BATCH","F_PROTOCOL","F_EB_EOD_ERROR","TAFJ","LOCK","OS_"]
            
        counter=1
        for tabwild in tablewildcardArray:
            if tabwild.strip().upper() != "":
                if counter==1 :
                    counter=counter+1
                    ignore= " and   "
                    ignore = ignore + "(   tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                    continue
                ignore = ignore + " or  tname not like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
        if ignore!= "":
            ignore= ignore+ " )  "        
    
        return ignore      
    
    
    
    
    def getimptables(this, tname):
        ignore =  ""
        imptable = this.oracleGettablefrompgm(tname)
        imptable =[["FBE1_LIMIT"]]
        counter=1
        for tabname in imptable:
            tabwild = tabname[0]
            if tabwild.strip().upper() != "":
                if counter==1 :
                    counter=counter+1
                    ignore= " and    "
                    ignore = ignore + " (  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
                    continue
                ignore = ignore + " or  tname like '%"+ tabwild.replace('.','_').replace('_','\_').strip().upper()+"%'   ESCAPE '\\' "
        if ignore!= "":
            ignore= ignore+ " )  "    
        
        return ignore            
 
 