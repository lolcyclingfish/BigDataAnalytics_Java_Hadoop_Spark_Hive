import os

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName='Simple App')
sqlContext = SQLContext(sc)

sc.setLogLevel("ERROR")

#read data as dataframe
crime = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs://wolf.iems.northwestern.edu/user/huser1/crimeSample.csv')
crime.show(10)

print(crime.printSchema())

#read data as RDD and convert to dataframe -- must remove header
crimeRDD = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/huser1/crimeSample.csv')
header = crimeRDD.first()
crimeRDD = crimeRDD.filter(lambda x: x!= header).map(lambda x: x.split(","))
crimeDF = sqlContext.createDataFrame(crimeRDD,['ID','Case Number','Date','Block','IUCR','Primary Type','Description','Location Description','Arrest','Domestic','Beat','District','Ward','Community Area','FBI Code','X Coordinate','Y Coordinate','Year','Updated On','Latitude','Longitude','Location'])

crimeDF.show(5)
