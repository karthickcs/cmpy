import logging
import os
import psycopg2

class CommonDao:

    def __init__(this, logger_info,logger_debug, database, username, password, host, port):
        this.logger_debug = logger_debug
        this.logger_info = logger_info
        this.database = database
        this.username = username
        this.password = password
        this.host = host
        this.port = port
        this.prrowconnection = None
        this.connection = None

    def connect(this):
        this.connection = psycopg2.connect(
            database=this.database,
            user=this.username,
            password=this.password,
            host=this.host,
            port=this.port,
        )
        this.cursor=this.connection.cursor()
        
    def connectprrow(this):
        this.prrowconnection = psycopg2.connect(
            database=this.database,
            user=this.username,
            password=this.password,
            host=this.host,
            port=this.port,
        )
        this.prrowcursor=this.prrowconnection.cursor()    
        
        

    def closeprrowConnection(this):
         
        if this.prrowcursor:
            this.prrowcursor.close()
        if this.prrowconnection:
            this.prrowconnection.close()
            
    def closeConnection(this):
        this.connection.commit() 
        if this.cursor:
            this.cursor.close()
        if this.connection:
            this.connection.close()        

   

