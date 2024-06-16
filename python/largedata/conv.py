import os
#*****************************
str="4.7.14250"

#************************************
os.system('cls')
org="1234567890"
rep="9876543210"
ans=""
for i in range(len(str)) :
    
    if str[i]==".":
        ans+="."
    else:    
        ans+=rep[org.find(str[i])]
    
print(ans)    
    