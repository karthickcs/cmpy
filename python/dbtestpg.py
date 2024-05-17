#!/usr/bin/env python
# coding: utf-8

# In[3]:


# importing module
import psycopg2
import pandas as pd

import json
import os
from lxml import etree,objectify
import sys 
from operator import length_hint 
import dictdiffer 
import time
#from DataProcessor_0415pg import mainProgram
 
def tableListener():
    cursor = None
    con = None
    
    try:
         
        con = psycopg2.connect(database="postgres", user="postgres", password="password", host="localhost", port=5432)
        cursor = con.cursor()
        statement="select * from DP_LISTEN_TABLE where status='CREATED'  and (select count(status) from DP_LISTEN_TABLE where status='INPROGRESS' ) = 0 order by insertts"
        cursor.execute(statement)    		
        results = cursor.fetchone()
        if results == None :
            return
        upstatement ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'INPROGRESS\'  where Taskid=\''+str(results[1]) +'\' and runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement)
        con.commit()
        print('success  ',results)
        #mainProgram(results[2],results[8],results[6],results[1],results[7])
        upstatement2 ='update DP_LISTEN_TABLE set UPDATETS=Current_timestamp,status=\'DONE\'  where Taskid=\''+str(results[1]) +'\' and  runid=\''+str(results[7]) +'\''
        #print (upstatement)
        cursor.execute(upstatement2)
        con.commit()
    except psycopg2.DatabaseError as e:
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
        print("Waiting**********----->")
        time.sleep(10.0)
os.system('cls')
print('hi')
sleepLoop()
  

print('completed')
 


