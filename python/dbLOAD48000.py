#!/usr/bin/env python
# coding: utf-8

# In[3]:

import cx_Oracle
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
import psycopg2
import concurrent.futures
import pandas as pd
import json
import difflib 
import threading
import time
from lxml import etree,objectify
import os
import traceback
import sys 
import html
from operator import length_hint 
import dictdiffer 
from collections import OrderedDict
import numpy as np
from collections import defaultdict
# for now()
import datetime
import csv 
# for timezone()
import pytz
from threading import Thread
import psycopg2
import concurrent.futures
import pandas as pd
import json
import difflib 
import threading
import time
from lxml import etree,objectify
import os
import traceback
import sys 
import html
from operator import length_hint 
import dictdiffer 
from collections import OrderedDict
import numpy as np
from collections import defaultdict
# for now()
import datetime
import csv 
# for timezone()
import pytz
from threading import Thread

# In[2]:

def insertProws(lines):
    cursor = None
    con = None
    
    try:
        con = cx_Oracle.connect("oldsys/password@localhost:1521/XEPDB1")
        cursor = con.cursor()
        statement="Insert into LOG_TABLE (TNAME,ROWIDVAL,SCNNUM,INFORMATION,UPDATETS) values ('"+lines[0]+"','"+lines[1]+"','"+lines[2]+"','"+lines[3]+"',to_timestamp('"+lines[4]+"','DD-MM-RR HH.MI.SSXFF AM'))";
        cursor.execute(statement)    		
         
        
        con.commit()
        
    except Exception:
        print("There is a problem with Oracle+++++++")
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
            
def readcsvdata(fname,row):
    totalerr=""
    datadict={}
    datadicttemp={}
    cvalold=-1
    i=0
    rval=0
    try:
        with open(fname, mode ='r')as file:
            csvFile = csv.reader(file)
            for lines in csvFile:
                if i==0 :
                    i=i+1
                    continue
                insertProws(lines)
    except Exception:
        traceback.print_exc()
    print(totalerr)
    return datadict
 

 
cx_Oracle.init_oracle_client(lib_dir=r"C:\app\cskar\product\21c\dbhomeXE\bin")
readcsvdata('book1.csv',50000) 

print('completed')
 


