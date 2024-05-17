#!/usr/bin/env python
# coding: utf-8

# In[3]:


# importing module
import cx_Oracle
import pandas as pd
import xlwings as xw
import json
import os
from lxml import etree,objectify
import sys 
from operator import length_hint 
import dictdiffer 
import time
#from DataProcessor_0415 import mainProgram

cx_Oracle.init_oracle_client(lib_dir=r"C:\app\cskar\product\21c\dbhomeXE\bin")

def tableListener():
    cursor = None
    con = None
    
    try:
        con = cx_Oracle.connect('myuser/password@localhost:1521/XEPDB1')
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where status='CREATED'  and (select count(status) from DP_LISTEN_TABLE where status='INPROGRESS' ) = 0 order by insertts"
        cursor.execute(statement)    		
        results = cursor.fetchone()
        if results == None :
            return
        upstatement ='update DP_LISTEN_TABLE set UPDATETS=sysdate,status=\'INPROGRESS\'  where Taskid=\''+results[1] +'\''
        #print (upstatement)
        cursor.execute(upstatement)
        con.commit()
        print('success  ',results)
        #mainProgram(results[2],results[6],results[1])
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=sysdate,status=\'DONE\'  where Taskid=\''+results[1] +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle+++++++", e)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

 


 

# a loop to run the check every 2 seconds- as to lessen cpu usage
def sleepLoop():
    while 0 == 0:
        tableListener()
        time.sleep(2.0)
os.system('cls')
sleepLoop()
  

print('completed')
 


