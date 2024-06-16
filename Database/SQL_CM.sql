

create table LOG_TABLE_clob(
TNAME             VARCHAR2(500) ,
ROWIDVAL          VARCHAR2(500) ,
SCNNUM            VARCHAR2(500) ,
INFORMATION       clob   ,
UPDATETS          TIMESTAMP(6)  
)


create table LOG_TABLE(
TNAME             VARCHAR2(500) ,
ROWIDVAL          VARCHAR2(500) ,
SCNNUM            VARCHAR2(500) ,
INFORMATION       xmltype   ,
UPDATETS          TIMESTAMP(6)  
)


-- XML SQL
SELECT  table_name 
        FROM all_tab_columns 
        WHERE table_name like 'F%' and table_name not like '%JOB_LIST%' and table_name not like '%PARAM%' and table_name not like '%TSA%' and column_name LIKE '%XMLR%' and data_type LIKE '%XMLTYPE%';

-- CLOB SQL

SELECT  table_name 
        FROM all_tab_columns 
        WHERE table_name like 'F%' and table_name not like '%JOB_LIST%' and table_name not like '%PARAM%' and table_name not like '%TSA%' and column_name LIKE '%XMLR%' and data_type LIKE '%CLOB%';


-- XML DATA

DECLARE
     CURSOR all_tables
     IS
        SELECT  table_name 
        FROM all_tab_columns 
        WHERE table_name like 'F%' and table_name not like '%JOB_LIST%' and table_name not like '%PARAM%' and column_name LIKE '%XMLR%' and data_type LIKE '%XMLTYPE%' ;
 
      v_id   NUMBER := 1;
  BEGIN
     FOR rec_cur IN all_tables
     LOOP
    BEGIN 
   EXECUTE IMMEDIATE   'create or replace trigger CHECKMATETRG_'
                          || rec_cur.table_name
                          || '
  after insert or update or delete on '
                          || rec_cur.table_name
                          || '
  FOR EACH ROW 
  declare
  begin
   
  if inserting then
     insert into LOG_TABLE values(''INSERTING on '||rec_cur.table_name||''',:NEW.RECID  ,  dbms_transaction.local_transaction_id ,:NEW.XMLRECORD,CURRENT_TIMESTAMP);
  elsif updating then
     insert into LOG_TABLE values(''UPDATING on '||rec_cur.table_name||''',:NEW.RECID  ,  dbms_transaction.local_transaction_id ,:NEW.XMLRECORD,CURRENT_TIMESTAMP);
   elsif deleting then
     insert into LOG_TABLE values(''DELETING on '||rec_cur.table_name||''',:OLD.RECID  ,  dbms_transaction.local_transaction_id ,:OLD.XMLRECORD,CURRENT_TIMESTAMP);
   end if;
   
  end;';
  v_id := v_id +1;
  EXCEPTION 
      WHEN OTHERS 
        THEN dbms_output.put_line(SQLCODE);
  end;
     END LOOP;
     dbms_output.put_line(v_id);
  END;
  

-- clob data
  

DECLARE
     CURSOR all_tables
     IS
        SELECT  table_name 
        FROM all_tab_columns 
        WHERE table_name like 'F%' and table_name not like '%JOB_LIST%' and table_name not like '%PARAM%' and column_name LIKE '%XMLR%' and data_type LIKE '%CLOB%';
 
      v_id   NUMBER := 1;
  BEGIN
     FOR rec_cur IN all_tables
     LOOP
    BEGIN 
   EXECUTE IMMEDIATE   'create or replace trigger CHECKMATETRG_'
                          || rec_cur.table_name
                          || '
  after insert or update or delete on '
                          || rec_cur.table_name
                          || '
  FOR EACH ROW 
  declare
  begin
   
  if inserting then
     insert into LOG_TABLE_clob values(''INSERTING on '||rec_cur.table_name||''',:NEW.RECID  ,  dbms_transaction.local_transaction_id ,:NEW.XMLRECORD,CURRENT_TIMESTAMP);
  elsif updating then
     insert into LOG_TABLE_CLOB values(''UPDATING on '||rec_cur.table_name||''',:NEW.RECID  ,  dbms_transaction.local_transaction_id ,:NEW.XMLRECORD,CURRENT_TIMESTAMP);
   elsif deleting then
     insert into LOG_TABLE_CLOB values(''DELETING on '||rec_cur.table_name||''',:OLD.RECID  ,  dbms_transaction.local_transaction_id ,:OLD.XMLRECORD,CURRENT_TIMESTAMP);
   end if;
   
  end;';
  v_id := v_id +1;
  EXCEPTION 
      WHEN OTHERS 
        THEN dbms_output.put_line(SQLCODE);
  end;
     END LOOP;
     dbms_output.put_line(v_id);
  END;
  


  DECLARE
     CURSOR all_tables
     IS
      select 'drop trigger ' || trigger_name  stmt from user_triggers where trigger_name like '%CHECKMATETRG%';
 
      v_id   NUMBER := 1;
  BEGIN
     FOR rec_cur IN all_tables
     LOOP
    BEGIN 
    dbms_output.put_line( rec_cur.stmt);
   EXECUTE IMMEDIATE  rec_cur.stmt;
    
  v_id := v_id +1;
  EXCEPTION 
      WHEN OTHERS 
        THEN dbms_output.put_line(SQLERRM );
  end;
     END LOOP;
     dbms_output.put_line(v_id);
  END;
  
  
  
  
  
  
  create table cinfo(
TNAME             VARCHAR2(500) ,
cname          VARCHAR2(500) ,
aname            VARCHAR2(500) 
);
 
	  
	   
DECLARE
    CURSOR all_tables IS
    SELECT
        table_name
    FROM
        all_tab_columns
    WHERE
        table_name LIKE 'D!_%' ESCAPE '!'
        AND data_type LIKE '%CLOB%';

    v_id NUMBER := 1;
BEGIN
    FOR rec_cur IN all_tables LOOP
        BEGIN 
        EXECUTE IMMEDIATE 'insert into cinfo select '''||rec_cur.table_name||''', RECID,''C''||SUBSTR( XMLRECORD,INSTR(XMLRECORD,'''',1,1)+1,INSTR(XMLRECORD,'''',1,2)-(INSTR(XMLRECORD,'''',1,1)+1)) from '||rec_cur.table_name||'  ' ;
       dbms_output.put_line('insert into cinfo select '''||rec_cur.table_name||''', RECID,''C''||SUBSTR( XMLRECORD,INSTR(XMLRECORD,'''',1,1)+1,INSTR(XMLRECORD,'''',1,2)-(INSTR(XMLRECORD,'''',1,1)+1)) from '||rec_cur.table_name||';'); 
   --EXECUTE IMMEDIATE     rec_cur.table_name ;
            v_id := v_id + 1;
        EXCEPTION
            WHEN OTHERS THEN
                dbms_output.put_line(sqlcode);
        END;
    END LOOP;

    dbms_output.put_line(v_id);
END;