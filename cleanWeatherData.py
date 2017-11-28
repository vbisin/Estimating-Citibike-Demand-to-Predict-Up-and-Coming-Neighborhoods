import numpy as np


## import data
data=np.genfromtxt('weatherData.txt',delimiter=',')

iterator=np.arange(1009)
nanColumns=set()

## I) check if column is nan (i.e. was string but converted into double)
for column in iterator:
    if np.isnan(data[:,column]).any():
        nanColumns.add(column)
        
           
remainingColumns=set(iterator)-nanColumns      
           

## II) check if columns have nonzero variance
    
zeroStdColumns=set()

for column in np.array(list(remainingColumns)):           
    if np.std(data[:,column])<.1:
        zeroStdColumns.add(column)
 
columnsToDelete=nanColumns.union(zeroStdColumns)

remainingColumns=set(np.arange(1009))-set(columnsToDelete)
remainingColumns=np.array(list(remainingColumns))


## III) Check if columns have mainly indicator variables (i.e. 9, 99, 999, 9999, 99999)
    
deletedColumns=0
    
for column in remainingColumns:
    indicatorCounter=0
    
    for row in range(47335):
        if (data[row,column]==9 or data[row,column]==99 or data[row,column]==999 or data[row,column]==9999 or data[row,column]==99999 or data[row,column]==999999 \
           or data[row,column]==9.9 or data[row,column]==99.9 or data[row,column]==999.9 or data[row,column]==9999.9):
            indicatorCounter+=1
           # If too many indicator or missing variables, delete the column 
    if indicatorCounter>2000:
        columnsToDelete.add(column)
        deletedColumns+=1

remainingColumns=set(np.arange(1009))-columnsToDelete
remainingColumns=np.array(list(remainingColumns))

## IV)  Do some mean binning on the remaining columns (i.e. replace indicator variable with mean of two closest non indicator variables)

columnCounter=0

for column in remainingColumns:
    for row in range(47335):
         if (data[row,column]==9 or data[row,column]==99 or data[row,column]==999 or data[row,column]==9999 or data[row,column]==99999 or \
             data[row,column]==999999 or data[row,column]==999.9):
    
             if row!=0 and row!=len(data)-1:
                 
                 rowIncrease=row
                 rowDecrease=row
                 while (data[rowIncrease,column]==9 or data[rowIncrease,column]==99 or data[rowIncrease,column]==999 or data[rowIncrease,column]==9999 or \
                 data[rowIncrease,column]==99999 or data[rowIncrease,column]==999999 or data[rowIncrease,column]==999.9):    
                     rowIncrease+=1
                     
                 
                    
                 while (data[rowDecrease,column]==9 or data[rowDecrease,column]==99 or data[rowDecrease,column]==999 or data[rowDecrease,column]==9999 or \
                 data[rowDecrease,column]==99999 or data[rowDecrease,column]==999999 or data[rowDecrease,column]==999.9):                            
                     rowDecrease-=1
                
                 data[row,column]=np.mean([data[rowIncrease,column],data[rowDecrease,column]])
                
    
        
        # I.e. boundary conditions (upper or lower)         
             else:             
                 
                 if row==0:
                     rowIncrease=row
                     while (data[rowIncrease,column]==9 or data[rowIncrease,column]==99 or data[rowIncrease,column]==999 or data[rowIncrease,column]==9999 or \
                     data[rowIncrease,column]==99999 or data[rowIncrease,column]==999999 or data[rowIncrease,column]==999.9):    
                         rowIncrease+=1
                     
                     data[row,column]=data[rowIncrease,column]
                     
                 else: 
                     rowDecrease=row
                     while (data[rowDecrease,column]==9 or data[rowDecrease,column]==99 or data[rowDecrease,column]==999 or data[rowDecrease,column]==9999 or \
                     data[rowDecrease,column]==99999 or data[rowDecrease,column]==999999 or data[rowDecrease,column]==999.9):                            
                         rowDecrease-=1
                         
                     data[row,column]=data[rowDecrease,column]     
                    
                    
             

## V) recover the new data matrix 

columnsToDelete=np.array(list(columnsToDelete))       
remainingColumns=set(np.arange(1009))-set(columnsToDelete)
remainingColumns=np.sort(np.array(list(remainingColumns)))


cleanData=np.zeros((len(data),len(remainingColumns)))
counter=0
for column in remainingColumns:
    cleanData[:,counter]=data[:,column]
    counter+=1

## VI) Calculate covariance matrix

corrMatrix=np.corrcoef(np.transpose(cleanData))

## VII) Calculate SVD

(U,sigma,Vtrans)=np.linalg.svd(cleanData)                    
                         


## Save cleaned data for Pyspark
np.savetxt('cleanData.txt',cleanData)




################ 

## Now split weather data into weeks (using temp and rain)

import numpy as np

## import data
data=np.genfromtxt('weatherData.txt',delimiter=',')


## Bin the data that we want 
remainingColumns=[21]

for column in remainingColumns:
    for row in range(47335):
         if (data[row,column]==9 or data[row,column]==99 or data[row,column]==999 or data[row,column]==9999 or data[row,column]==99999 or \
             data[row,column]==999999 or data[row,column]==999.9):
    
             if row!=0 and row!=len(data)-1:
                 
                 rowIncrease=row
                 rowDecrease=row
                 while (data[rowIncrease,column]==9 or data[rowIncrease,column]==99 or data[rowIncrease,column]==999 or data[rowIncrease,column]==9999 or \
                 data[rowIncrease,column]==99999 or data[rowIncrease,column]==999999 or data[rowIncrease,column]==999.9):    
                     rowIncrease+=1
                     
                 
                    
                 while (data[rowDecrease,column]==9 or data[rowDecrease,column]==99 or data[rowDecrease,column]==999 or data[rowDecrease,column]==9999 or \
                 data[rowDecrease,column]==99999 or data[rowDecrease,column]==999999 or data[rowDecrease,column]==999.9):                            
                     rowDecrease-=1
                
                 data[row,column]=np.mean([data[rowIncrease,column],data[rowDecrease,column]])
                
    
        
        # I.e. boundary conditions (upper or lower)         
             else:             
                 
                 if row==0:
                     rowIncrease=row
                     while (data[rowIncrease,column]==9 or data[rowIncrease,column]==99 or data[rowIncrease,column]==999 or data[rowIncrease,column]==9999 or \
                     data[rowIncrease,column]==99999 or data[rowIncrease,column]==999999 or data[rowIncrease,column]==999.9):    
                         rowIncrease+=1
                     
                     data[row,column]=data[rowIncrease,column]
                     
                 else: 
                     rowDecrease=row
                     while (data[rowDecrease,column]==9 or data[rowDecrease,column]==99 or data[rowDecrease,column]==999 or data[rowDecrease,column]==9999 or \
                     data[rowDecrease,column]==99999 or data[rowDecrease,column]==999999 or data[rowDecrease,column]==999.9):                            
                         rowDecrease-=1
                         
                     data[row,column]=data[rowDecrease,column]     
  
for entry in range(len(data[:,32])):
    if data[entry,32]==999.9:
        data[entry,32]=0
        
                  
                    



temp=data[:,21][5762:45345]
rain=data[:,32][5762:45345]

dates=data[:,3].astype(int).astype(str)
dates=dates[5762:45345]
daySplitterTemp=list()
daySplitterRain=list()

counter=0

resultTemp=list()
resultRain=list()
entryList=list()

datesFinal=list()

for date in dates:
    entry=int(date[-2:])
    
    if counter==0:
        current=entry
        daySplitterTemp.append(temp[0])
        daySplitterRain.append(rain[0])
        datesFinal.append(date)
        entryList.append(entry)

        
    elif entry==current:
        daySplitterTemp.append(temp[counter])
        daySplitterRain.append(rain[counter])
        
    elif entry != current:
        resultTemp.append(np.mean(daySplitterTemp))
        resultRain.append(np.mean(daySplitterRain))
        daySplitterTemp=list()
        daySplitterRain=list()
        current=entry
        entryList.append(entry)
        datesFinal.append(date)

        
    if counter==len(dates)-1:
        resultTemp.append(np.mean(daySplitterTemp))
        resultRain.append(np.mean(daySplitterRain))
    
    counter+=1    
    
datesFinal=np.asarray(datesFinal).astype(int)   

 
toReturn=np.zeros((len(datesFinal),3))
toReturn[:,0]=datesFinal
toReturn[:,1]=resultTemp
toReturn[:,2]=resultRain       
        
(U,sigma,Vtrans)=np.linalg.svd(toReturn[:,1:])        
        
np.savetxt('weatherDataCleaned.txt',toReturn)
#data=pandas.read_csv('weatherDataCSV.csv')



