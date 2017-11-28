import pandas
import numpy as np
from dateutil import parser
import matplotlib.pyplot as plt


iterator=np.arange(40)+2
data=pandas.read_csv('1.csv')                  

for i in iterator:
    data1=pandas.read_csv(str(i)+'.csv')
    print(i)
    data=np.vstack((data,data1))
    
dates=data[:,1].astype(str)    
stationID=data[:,3].astype(int)
monthsToGraph=np.zeros(12)                    
stationIDToGraph=np.zeros(np.max(stationID))
## Graph demand per month and demand per station



for entry in range(len(data)):
    
    # demand per month 
    print(entry)
    d=parser.parse(dates[entry])
    entryMonth=d.strftime("%Y-%m-%d")
    entryMonth=int(entryMonth[5:7])
    
    monthsToGraph[entryMonth-1]+=1
    
    # demand per station
    ID=stationID[entry]
    
    stationIDToGraph[ID-1]+=1
    
monthNames=['Jan','Feb','Mar','Apr','May','June','July', 'Aug','Sept','Oct','Nov','Dec']    
months=np.arange(12)+1

                
## Then NEED TO TAKE AVERAGE                
                
divideByThree=[0,1,2,3,4,5]
divideByFour=[6,7,8,9,10,11]

for month in divideByThree:
    monthsToGraph[month]=monthsToGraph[month]/3.

for month in divideByFour:
    monthsToGraph[month]=monthsToGraph[month]/4.
                 
                
# demand per month                 
plt.figure(1)
plt.title("Average Demand per Year For Each Month")
plt.xticks(months, monthNames)
plt.ylabel('Demand')
plt.plot(months,monthsToGraph,'b')
plt.savefig('avgDemandPerMonth')

stationIDToGraph=np.ndarray.tolist(stationIDToGraph)
stations=[x for x in stationIDToGraph if x>0]

plt.figure(2)
plt.title("Total Demand For Each Station ID")
plt.xlabel('Station ID')
plt.ylabel('Demand')
plt.plot(np.arange(len(stations))+1,stations,'k')
plt.savefig('avgDemandPerID')
                 