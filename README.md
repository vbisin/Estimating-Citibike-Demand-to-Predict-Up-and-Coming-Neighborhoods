# Estimating Citibike Demand to Predict Up and Coming Neighborhoods

##1. Project-Report.pdf - PDF of written report for final project

##2. Estimating Citibike Demand.pdf - Slides for presentation of report to Big Data Science class Spring '17

##3. citibikeGraphs.py - This script plots the demand for each citibike station as can be seen in the graphs in the appendix.

##4. gaussian.py - This script runs on pySpark 2.1.0 and first loads the data then cleans the data and then finally produces the feature vectors. We then train a Gaussian General Linear Model and it produces the error rate for any station (which needs to be specified).

##5. cleanWeatherData.py - Cleans the NOAA weather dataset as described in the paper. Results in two usable features: temperature and rain. 

##6. graphResults.py - Determines and graphs the average error per bike station for each model.

##7. weatherGraphs.py - Plots the average temperature and rain per month given the NOAA dataset. 

##8. Citibike_clustering.py - This script uses K-Means clustering on the stations. Here the feature we cluster on is the number of trips between two stations, in a similar fashion as the PageRank section of the paper. 
