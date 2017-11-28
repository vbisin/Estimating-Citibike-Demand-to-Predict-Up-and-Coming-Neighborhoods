from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import avg
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import date
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt

def main(sc):
        sqlContext=SQLContext(sc)
        df= sqlContext.read.format("com.databricks.spark.csv").options(header= 'true',delimiter= ",").load("/user/pt1089/CitiBikeData/*.csv")
        df=df.select( 'start station id','end station id', 'starttime', 'tripduration')   #selecting required columns from the file
        datSplitterUDF = udf(lambda row : splitUDF(row),ArrayType(StringType())) 
        split_col = split(df['starttime'], ' ')                             
        df= df.withColumn('Date', split_col.getItem(0))         #creating time and date from timestamp
        df = df.withColumn('Time', split_col.getItem(1))
        df=df.withColumn('dt',datSplitterUDF(df.Date).alias("dt"))
        df=df.withColumn('year',col('dt').getItem(0).cast('int'))           #creating year month and other date features
        df=df.withColumn('month',col('dt').getItem(1).cast('int'))
        df=df.withColumn('day',col('dt').getItem(2).cast('int'))
        df=df.withColumn('weeknumber',col('dt').getItem(3).cast('int'))
        df=df.withColumn('weekday',col('dt').getItem(4).cast('int'))
        split_time=split(df['Time'], ':')
        df= df.withColumn('Hour', split_time.getItem(0).cast('int'))
        df= df.withColumn('Minutes', split_time.getItem(1).cast('int'))
        df= df.drop('starttime').drop('Date').drop('Time').drop('dt').drop('day')
        df1=df.groupby('start station id','end station id','month','year','weeknumber','weekday').agg(count('start station id')) #creating feature vector
        df1=df1.drop('month').drop('year')
        df1=df1.withColumn('startstationid',df['start station id'].cast("int"))
        df1=df1.withColumn('endstationid',df['end station id'].cast("int"))
        df1=df1.drop('start station id').drop('end station id')
        df3=df1.drop('startstationid').drop('endstationid')
        vectors=df3.map(lambda data: Vectors.dense([int(c) for c in data])) #feature vector
        clusters = KMeans.train(vectors, 5, maxIterations=10,runs=10,initializationMode="random") #Kmeans model
        rows=df1.map(lambda data: Vectors.dense([int(c) for c in data]))
        predictions=rows.map(lambda x: ([x[4],x[5]],clusters.predict(Vectors.dense(x[3]))))   #prediction for each row
        qwe = predictions.map(lambda x: [int(x[0][0]),int(x[0][1]),int(x[1])])
        df_predictions=qwe.toDF()
        df_new = df1.join(df_predictions, (df1.startstationid == df_predictions._1) & (df1.endstationid == df_predictions._2))
        dfcluster=df_new.groupby('_3').agg(mean('count(start station id)'))    #showcasing cluster results
        dfcluster.show()

def missingvals(df): #function to take care of missing values and erroneous values in birthyear
        df=df.withColumn('birthyear', F.when(df.birthyear<1900,df.birthyear+100).otherwise(df.birthyear))
        df=df.na.fill(df.na.drop().agg(avg("birthyear")).first()[0], ["birthyear"])
        return df

def splitUDF(row): # UD function to extract data features like month, data, year , weekday and weeknumber
        if "/" in row:
                mm,dd,yyyy = row.split("/")
        elif "-" in row:
                yyyy,mm,dd = row.split("-")
        mm,dd,yyyy = int(mm.replace('"', "")),int(dd.replace('"', "")),int(yyyy.replace('"', ""))
        weeknumber=datetime.date(yyyy,mm,dd).isocalendar()[1]
        weekdaynumber=datetime.date(yyyy,mm,dd).isoweekday()
        if weekdaynumber<6:
                weekday=1
        elif weekdaynumber>=6:
                weekday=0
        return [yyyy,mm,dd,weeknumber,weekday]

def cast_function(df):    #function to cast columns into correct datatypes
        df = df.withColumn("tripduration", df["tripduration"].cast("float"))
        df = df.withColumn("startstationid", df["start station id"].cast("integer"))
        df=df.drop('start station id')
        df = df.withColumn("startstationlatitude", df["start station latitude"].cast("float"))
        df=df.drop('start station latitude')
        df = df.withColumn("startstationlongitude", df["start station longitude"].cast("float"))
        df=df.drop('start station longitude')
        df = df.withColumn("endstationid", df["end station id"].cast("integer"))
        df=df.drop('end station id')
        df = df.withColumn("endstationlatitude", df["end station latitude"].cast("float"))
        df=df.drop('end station latitude')
        df = df.withColumn("endstationlongitude", df["end station longitude"].cast("float"))
        df=df.drop('end station longitude')
        df = df.withColumn("bikeid", df["bikeid"].cast("integer"))
        df = df.withColumn("birthyear", df["birth year"].cast("integer"))
        df=df.drop('birth year')
        df = df.withColumn("gender", df["gender"].cast("integer"))
        return df

if __name__ == "__main__":

   # Configure Spark
        conf = SparkConf().setAppName("My App")
        conf = conf.setMaster("local[*]")
        sc   = SparkContext(conf=conf)
   # Execute Main functionality
        main(sc)