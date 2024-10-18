import logging
import os
import oracledb
import psycopg2

class CommonOraDao:

    def __init__(this, logger_info,logger_debug, username, password, jdbcurl):
        this.logger_info = logger_info
        this.logger_debug = logger_debug
      
        this.username = username
        this.password = password
        this.jdbcurl = jdbcurl
        this.connection= None
        this.cursor=None
        
     
            
            

   

