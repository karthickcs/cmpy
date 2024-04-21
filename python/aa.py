import threading
class dataProcessorThread (threading.Thread):
   def __init__(self, name,start,end, tab1, tab2, counttab1 , counttab2):
      threading.Thread.__init__(self)
      self.name = name
      self.start= start
      self.end= end
      self.tab1 =tab1
      self.tab2 =tab2
      self.ctab1sorted =counttab1
      self.ctab2sorted =counttab2

   def run(self):
        try:        
            keysListtab1 = list(self.tab1.keys())
            #print(keysListtab1[self.sizeval:self.sizeval+10])
            i=0
            Masterlist=[]
            for kvalmaster in keysListtab1[self.sizeval:self.sizeval+99]:
                #if str(kvalmaster) != '1010074298' :
                    #continue
                i=i+1
                if i>10000 :
                    break
                sublist=[]
                compscore=sys.maxsize
                matchkey=0;
                master=self.tab1[kvalmaster ]
                filtered_dict = dict(filter(lambda item: self.ctab1sorted[kvalmaster]-10 <= item[1] <= self.ctab1sorted[kvalmaster]+10, self.ctab2sorted.items()))
                keysListtab2 = list(filtered_dict.keys())
                print(self.name,":",i,":",kvalmaster,";",len(filtered_dict),":**",len(master))
                for kval in keysListtab2:
                    #if str(kval) != '1010074298' :
                        #continue
                    follower=self.tab2[kval]
                    #print(kvalmaster)
                    #print(kval)
                    difflist = list(dictdiffer.diff(master, follower))
                    difflistlength=0
                    for t in difflist:
                        difflistlength=difflistlength+ len(t[2])
                    #print(list(dictdiffer.diff(master, follower)))
                    #print(kvalmaster,kval,difflistlength)
                    #print(difflistlength)
                    if difflistlength<compscore :
                        compscore=difflistlength
                        matchkey= kval
                    #if difflistlength<= 3 :
                        #break
                        #print(difflistlength)
                        #print(master)
                        #print(follower)
                #print(kvalmaster ,matchkey, compscore)
                
                sublist.append(list(dictdiffer.diff(master, tranDataTab2[matchkey])))
                sublist.append(kvalmaster )
                sublist.append(matchkey)
                sublist.append(compscore)
                Masterlist.append(sublist)
                
                
                
            df = pd.DataFrame(Masterlist)
            df.to_csv('Finaldiff.csv')
        except Exception as error:
            print("An exception occurred:", error)
def f():
    print ('thread function')
    return

if __name__ == '__main__':
    threadrunner =[]
    for i in range(3):
        t = threading.Thread(target=f)
        t.start()
        threadrunner.append(t)
        
    for x in threadrunner :
        x.join()    
    for i in range(10):
        print(i)    