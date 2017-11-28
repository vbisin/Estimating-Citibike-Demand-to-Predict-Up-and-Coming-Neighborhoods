import numpy as np
import matplotlib.pyplot as plt
data=np.genfromtxt('weatherDataCleaned.txt',delimiter=' ')


#Obtain month and group them together

currentMonthsTemp=list()
currentMonthsRain=list()
graphRain=list()
graphTemp=list()


months=np.arange(12)+1
dates=data[:,0].astype(int).astype(str)

for month in months:
    for entry in range(len(data)):
        entryMonth=int(dates[entry][-4:-2])
        #print(entryMonth)
        
        if entryMonth==month:
            currentMonthsTemp.append(data[entry,1])
            currentMonthsRain.append(data[entry,2])
    graphTemp.append(np.mean(currentMonthsTemp))     
    graphRain.append(np.mean(currentMonthsRain))
    currentMonthsTemp=list()
    currentMonthsRain=list()
            
monthsGraph=['Jan','Feb','Mar','Apr','May','June','July', 'Aug','Sept','Oct','Nov','Dec']    
    
plt.figure(1)
plt.title("Average Temperature per Month")
plt.xticks(months, monthsGraph)
plt.ylabel('Average Temperature (Celsius)')
plt.plot(months,graphTemp,'b')
plt.savefig('avgTempPerMonth')

plt.figure(2)
plt.title("Average Rain per Month")
plt.xticks(months, monthsGraph)
plt.ylabel('Average Rain (millimeters)')
plt.plot(months,graphRain,'k')
plt.savefig('avgRainPerMonth')




#plt.figure(1)
#plt.title("Average Alpha and W Step Sizes per Epoch")
#plt.plot(np.arange(len(learningRateAlphasAvg))+1,learningRateAlphasAvg,'b',label='Step Size Alpha')
#plt.plot(np.arange(len(learningRateWAvg))+1,learningRateWAvg,'r',label='Step Size W')
#plt.legend(bbox_to_anchor=(.52, .24), loc=0, borderaxespad=0.)
#plt.xlabel('Epoch')