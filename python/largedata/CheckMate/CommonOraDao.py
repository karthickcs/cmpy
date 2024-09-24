import logging
import os
import oracledb
import psycopg2

class CommonOraDao:

    def __init__(this, logger, username, password, jdbcurl):
        this.logger = logger
      
        this.username = username
        this.password = password
        this.jdbcurl = jdbcurl
        
    def connect(this):
        this.connection =  oracledb.connect(
        user=this.username,
        password=this.password,
        dsn=this.jdbcurl)
        
        this.cursor=this.connection.cursor()

    def closeConnection(this):
        this.connection.commit() 
        if this.cursor:
            this.cursor.close()
        if this.connection:
            this.connection.close()

   

