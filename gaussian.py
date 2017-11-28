from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.ml.regression import LinearRegression
import numpy as np
from math import sqrt
import datetime
from datetime import date
from pyspark.sql.types import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.mllib.regression import LabeledPoint, IsotonicRegression, IsotonicRegressionModel
import math
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import GeneralizedLinearRegression

folder = "/user/rs4070/mydata/*.txt"

"""
These are functions we use for mapping and reducing, primarily to access, clean, and then finally use it in our model.
"""
def func(x):
	date,stat = x[0],x[1]
	arr = date.split(" ")
	if len(arr)>1 and arr[0]!="Start":
		a1,a2 = arr[0],arr[1]
		return [a1,a2,stat]
	else:
		return [date,stat]

def f(x):
	if len(x)<3:
		return x
	else:
		date,time,stat = x[0],x[1],x[2]
		hour,minute = x[1].split(":")[0],x[1].split(":")[1]
		hour,minute = int(hour.replace('"', "")),int(minute.replace('"', ""))
		new_date = date.split("/")
		if len(new_date)>1:
			month,day,year=new_date
			month,day,year = int(month.replace('"', "")),int(day.replace('"', "")),int(year.replace('"', ""))
			weekNumber = datetime.date(year,month,day).isocalendar()[1]
			buffer1,weekend = datetime.date(year,month,day).weekday(),0
			if buffer1<5: weekend =1
		else:
			old_date = date.split("-")
			year,month,day=old_date
			year,month,day = int(year.replace('"', "")),int(month.replace('"', "")),int(day.replace('"', ""))
			weekNumber = datetime.date(year,month,day).isocalendar()[1]
			buffer1,weekend = datetime.date(year,month,day).weekday(),0
			if buffer1<5: weekend =1
		return [day,month,year,hour,int(stat.replace('"', "")),weekNumber,weekend,1]

def mapp(x):
	if x[0]==header[0] or x[0]=="Start Time" or x[0] == "starttime":
		return ["day","month","year","hour","stat","weekNumber","weekend","demand"]
	else:
		return x


datapoint = ["tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","usertype","birth year","gender"]

data = sc.textFile(folder)

"""
Here we apply the above functions on the datafolder. 
"""
parsedData = data.map(lambda line: ([(x) for x in line.split(',')])).map(lambda x: ([x[1],x[3]]))

header = parsedData.take(1)[0]

rdd = parsedData.map(lambda x:func(x)).map(lambda x:f(x)).map(lambda x:mapp(x))
header1 = rdd.take(1)[0]
ar1 = rdd.take(2)[1]

def removeHeader(x):
	if x[0]==header1[0]:
		return ar1
	else:
		return x

rdd1 = rdd.map(lambda x:removeHeader(x))

#Transform the intial RDD into dataframe
schemaString = header1
fields = [StructField(field_name, IntegerType(), True) for field_name in schemaString]
schema = StructType(fields)

df = sqlContext.createDataFrame(rdd1, schema)

df1 = df.groupBy("day","month","year","stat","weekNumber","weekend").count()
df1 = df1.drop(df1.day).drop("year").drop("day").drop("month")

# this creates the final feature vector for one station ( station number 320)
df333 = df1.filter("stat=320").drop("stat")
new_rdd = df333.rdd

# Spark Mllib uses a particular format for the input, which is similar to libsvm. 
def labell(x):
	return (Vectors.dense([x[0],x[1]]),x[-1])

my_rdd = new_rdd.map(lambda x:labell(x))
dataset = spark.createDataFrame(my_rdd,["features", "label"])

# here we split the data into a training and test set
training, test = dataset.randomSplit([0.6, 0.4], 11)

# this defines the model that we will train the data on
glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)

model = glr.fit(training)

# the function model.evaluate essentially applies the model on new datapoints
arr = model.evaluate(test)
b = arr.predictions.collect()

# the function below produces the mean absolute error
def err(x):
     sum = 0 
     for arr in x:
            sum+= abs(arr.prediction - arr.label)/float(len(x))
     return sum
e = err(b)

print(e)
