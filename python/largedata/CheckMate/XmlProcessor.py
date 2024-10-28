import json
import logging
import os
import psycopg2
from lxml import etree,objectify
class XmlProcessor:
    def __init__(this, logger_info,logger_debug ):
        this.logger_info = logger_info
        this.logger_debug = logger_debug
        
        
                
                 
    def getXMLData(this,scndetails,batch,realtname,ignoretable,difftableStruct,type,taskid,runid,SuperArray):
        totalerr=""
        datadict={}
        datadicttemp={}
        cvalold=-1
        i=0
        rval=0
        cval=""
             
        for   row in scndetails:
            try: 
                cval=row[0]+"|"+row[1]+"|"+row[2]
                if  cval in SuperArray :
                    continue
                datadicttemp={}
                
                realTnameVal= row[0].split(" ")[2]  
                substring_start = realTnameVal[:realTnameVal.find('_')+1]
                substring_end = realTnameVal[realTnameVal.find('_')+1:] 
                realtnameResult=realtname[substring_end]
                try:
                    if type=='Src':                    
                        columnnames= difftableStruct[realtnameResult[0]][0]   
                    else:
                        columnnames= difftableStruct[realtnameResult[0]][0]     
                except Exception:
                    columnnames = ""        
                if cval in datadict:
                    datadict[cval]=this.readXmlDataFromStr(row[3],datadict[cval],row[0] ,realTnameVal,columnnames,ignoretable )
                else:
                    datadict[cval]=this.readXmlDataFromStr(row[3],datadicttemp,row[0] ,realTnameVal,columnnames,ignoretable  )
                i=i+1
                # print("processed:::"+str(i)+ ":::Len:::"+str(len(datadict)))
                rval=rval+1
            except Exception:
                this.logger_debug.error("STANDARD.SELECTION Record  not found "+realTnameVal)
              
        # this.logger_debug.debug(totalerr)
        return datadict

    def readXmlDataFromStr(this,text,datadict,tname ,realTnameVal,columnnames,ignoretable):

        try:
            ccc=0
            if text.find("</row>") < 0 :
                text = text[0:text.rfind(">\n")+1]+"</row>"
                
            root = etree.fromstring(text).getroottree()
            ns = {'df': 'http://www.w3.org/TR/html4/', 'types': 'http://www.w3schools.com/furniture'}
            for e in root.iter():
            
                path = root.getelementpath(e)
                root_path =  '/' + root.getroot().tag
                if path == '.':
                    path = root_path
                else:
                    path = root_path + '/' + path
                for ns_key in ns:
                    path = path.replace('{' + ns[ns_key] + '}', ns_key + ':')
                
                r = root.xpath(path, namespaces=ns)
                
                for x in r:  
                    sval=str(x.attrib).replace("{","[").replace("}","]")   
                    if len(path.split("/"))==3 :  
                        try:
                            columnnamesjson = json.loads(columnnames)
                            orgcol=path.split("/")[2].split("[")[0].upper()
                            appendcol=""
                            if len(path.split("/")[2].split("["))>1 :
                                appendcol="["+path.split("/")[2].split("[")[1]
                            col=columnnamesjson[orgcol].split("|")[0]
                            if not ('ALL.'+ col) in ignoretable and not (realTnameVal+'.'+ col) in ignoretable:                        
                                datadict[tname+"::"+path.replace(path.split("/")[2],col+appendcol
                                                                 )]=x.text
                            # else :
                                # print("column skipped------"+ col)    
                        except Exception:
                            datadict[tname+"::"+path]=x.text
                    
                    else:
                         datadict[tname+"::"+path]=x.text        
        except Exception:
            err =1
        
        return datadict

    
    def getXMLDataforFile(this,scndetails,batch ):
        totalerr=""
        datadict={}
        datadicttemp={}
        cvalold=-1
        i=0
        rval=0
        cval=""
             
        for   row in scndetails:
            try: 
                cval=row
                datadicttemp={}
               
                 
                 
                if cval in datadict:
                    datadict[cval]=this.readXmlDataFromStrfile(scndetails[row],datadict[cval],row )
                else:
                    datadict[cval]=this.readXmlDataFromStrfile(scndetails[row],datadicttemp,row)
                i=i+1
                
                rval=rval+1
            except Exception:
                this.logger_debug.debug("Exception"+batch)
                this.logger_debug.debug("exception ",exc_info=1)
        this.logger_info.info(totalerr)
        return datadict

    def readXmlDataFromStrfile(this,text,datadict,tname  ):

        try:
           
                
            root = etree.fromstring(text).getroottree()
            ns = {'df': 'http://www.w3.org/TR/html4/', 'types': 'http://www.w3schools.com/furniture'}
            for e in root.iter():
            
                path = root.getelementpath(e)
                root_path =  '/' + root.getroot().tag
                if path == '.':
                    path = root_path
                else:
                    path = root_path + '/' + path
                for ns_key in ns:
                    path = path.replace('{' + ns[ns_key] + '}', ns_key + ':')
                
                r = root.xpath(path, namespaces=ns)
                
                for x in r:  
                    sval=str(x.attrib).replace("{","[").replace("}","]")   
                     
                    datadict[path]=x.text        
        except Exception:
            err =1
        
        return datadict


    def getxmlfromclobdata(this,text):
        root = etree.Element("root")
        try:
        
            row = text.split("")
            root = etree.Element("root")
            child = etree.SubElement(root, "row")
            i=1
            for value in row:
                etree.SubElement(child, "pos"+str(i)).text = value
                i=i+1
        except:
            this.logger_debug.debug("exception ",exc_info=1)   
            
        #print (etree.tostring(root, encoding="unicode"))
        return etree.tostring(root, encoding='utf-8')



    def getXMLDataforclob(this,scndetails,batch):
        totalerr=""
        datadict={}
        datadicttemp={}
        cvalold=-1
        i=0
        rval=0
        cval=""
              
        for   row in scndetails:
            try: 
        
                cval=row[0]+"|"+row[1]+"|"+row[2]
                datadicttemp={}
                if(cval != cvalold) :
                    rval=1
                    cvalold=cval            
                if cval in datadict:
                    datadict[cval]=this.getxmlfromclobdata(row[3])
                else:
                    datadict[cval]=this.getxmlfromclobdata(row[3])
                i=i+1
                rval=rval+1
                # print("row processed"+str(i)+"dddddd::"+str(len(datadict)))
            except Exception:
                this.logger_debug.debug("Exception"+batch)
                this.logger_debug.debug("exception ",exc_info=1)
        this.logger_info.info(totalerr)
        return datadict

    # def skipifalreadyProcesssed(this,cval,taskid,runid):
         
    #     with open('Finaldiff_'+ str(taskid)+'_'+str(runid) +'.csv', 'r') as f:
    #         lines = f.readlines()
    #         cnt = 0

    #         for entry in lines:
    #             cnt=cnt+1
    #             if cval in entry:
    #                 this.logger_debug.debug("Row skipped::"+cval)
    #                 return True
    #     return False        
                 