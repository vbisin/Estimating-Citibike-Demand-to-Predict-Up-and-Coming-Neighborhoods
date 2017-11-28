import numpy as np
import pandas as pd
iterator=np.arange(32)+1
errorToGraph=list()
from os import listdir
import matplotlib.pyplot as plt


files = [f for f in listdir('.')]
files = [f for f in files if f[-3:]=='csv']
                 
for dataset in files:
    data=pd.read_csv(dataset)
    errorPerDataset=list()
    totalBikesPerDataset=list()
    for i in range(len(data)):
        errorPerDataset.append(abs(data['label'][i]-data['prediction'][i]))
        totalBikesPerDataset.append((data['label'][i]**2+data['prediction'][i]**2)**.5)
    errorToGraph.append(np.mean(errorPerDataset)/np.mean(totalBikesPerDataset))
    
    
plt.figure(1)
plt.title("Average GLM Error Without Weather Data")
plt.xlabel('Bike Station IDs')
plt.ylabel('Average Normalized Error')
plt.plot(errorToGraph,'k')
plt.savefig('avgErrorWithoutWeather')

np.mean(errorToGraph)
