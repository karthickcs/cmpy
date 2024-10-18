
import traceback
import dictdiffer
import DiffTablestructDao
from lxml import etree,objectify
import logging
import os
import psycopg2

class MetaDataGen:

    def generateMetaDataDifference(this,diffTablestructObj:DiffTablestructDao.DiffTablestructDao ,tabdatold,tabdatnew,taskid,runid,realtname):
        totalerr=""       
        
        try:         
            datadict={}            
            newtname = ''
            newrecord =''
            diffTablestructObj.connect()
            for keynew in  list(tabdatnew.keys()):
                newtname=tabdatnew[keynew][0]
                newrecord=tabdatnew[keynew][1]
                oldtname = ''
                oldrecord =''
                
                if keynew in tabdatold:
                    continue 
                               
                oldjson={}
                newjson=this.generteTableJson(newrecord)
                difference={"tablemissing": "SrcSystem"}
                diffTablestructObj.insertRecords(taskid,runid,newtname, oldjson,newjson,difference)  
                diffTablestructObj.connection.commit()  
            for keyold in  list(tabdatold.keys()):
                #this.logger_debug.debug("Comparing --> Meta data -->"+keyold) 
                oldtname=tabdatold[keyold][0]
                oldrecord=tabdatold[keyold][1]
                newtname = ''
                newrecord =''
                oldjson=this.generteTableJson(oldrecord)
                if keyold in tabdatnew :
                    newtname =  tabdatnew[keyold][0]
                    newrecord = tabdatnew[keyold][1] 
                    newjson=this.generteTableJson(newrecord)
                    difference= list(dictdiffer.diff(oldjson, newjson))
                else :
                    newjson={}
                    difference={"tablemissing": "TargetSystem"}               
                
                
                diffTablestructObj.insertRecords(taskid,runid,oldtname, oldjson,newjson,difference)
                diffTablestructObj.connection.commit()  
               
             
                
        except Exception:
            traceback.print_exc()
        finally:
            diffTablestructObj.closeConnection();  
        print(totalerr)
        return datadict

    def generteTableJson(this,xmltext):
        mytemp={}
        idict={}
        errval=0
        datadict={}
        try:
            root = etree.fromstring(xmltext).getroottree()
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
                path=path.replace("/row/","")
                path=path[0:path.find("[")]
                for x in r:
                    try:
                        if path.find("c3") > -1 and  x.text and  x.text.isnumeric(): 
                            att=x.attrib.keys() 
                            if len(att) ==0 :
                                datadict[path]=x.text
                            else:
                                aname=x.attrib.keys()[0]
                                if aname.find("id") > -1:
                                    datadict["tablename"]=x.attrib[aname]   
                                else:
                                    #datadict[path+"_"+str(aname)+"_"+x.attrib[aname]]=x.text
                                    datadict["C"+x.text] =mytemp["Main"+x.attrib[aname]]
                                    idict[x.attrib[aname]]=x.text
                        if path.find("c4") > -1 and  x.text  : 
                            att=x.attrib.keys() 
                            if len(att) ==0 :
                                datadict[path]=x.text
                            else:
                                aname=x.attrib.keys()[0]
                                if aname.find("id") > -1:
                                    datadict["tablename"]=x.attrib[aname]   
                                else:
                                    try:        
                                        datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                                    except Exception:
                                        errval=errval+1 
                        if path.find("c6") > -1 and  x.text  : 
                            att=x.attrib.keys() 
                            if len(att) ==0 :
                                datadict[path]=x.text
                            else:
                                aname=x.attrib.keys()[0]
                                if aname.find("id") > -1:
                                    datadict["tablename"]=x.attrib[aname]   
                                else:
                                    try:        
                                        datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                                    except Exception:
                                        errval=errval+1 
                        if path.find("c10") > -1 and  x.text  : 
                            att=x.attrib.keys() 
                            if len(att) ==0 :
                                datadict[path]=x.text
                            else:
                                aname=x.attrib.keys()[0]
                                if aname.find("id") > -1:
                                    datadict["tablename"]=x.attrib[aname]   
                                else:
                                    try:        
                                        datadict["C"+idict[x.attrib[aname]]] =datadict["C"+idict[x.attrib[aname]]]+"|"+x.text                                 
                                    except Exception:
                                        errval=errval+1 
                                    
                        if path.find("c1") > -1 : 
                            att=x.attrib.keys() 
                            if len(att) ==0 :
                                mytemp[path]=x.text
                            else:
                                aname=x.attrib.keys()[0]
                                if aname.find("id") > -1:
                                    mytemp["tablename"]=x.attrib[aname]   
                                else:
                                    #datadict[path+"_"+str(aname)+"_"+x.attrib[aname]]=x.text
                                    mytemp["Main"+x.attrib[aname]] =x.text           
                    except Exception:
                        err=1                
                                
        except Exception:
            traceback.print_exc()
            print()
        return datadict
        