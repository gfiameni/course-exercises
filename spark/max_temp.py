import re
import sys

from pyspark import SparkContext

#function to extract the data from the line
#based on position and filter out the invalid records
def extractData(line):
    val = line.strip()
        (year, temp, q) = (val[15:19], val[87:92], val[92:93])
            if (temp != "+9999" and re.match("[01459]", q)):
                            return [(year, temp)]
                                else:
                                            return []

#Create Spark Context with the master details and the application name

sc = SparkContext(appName="PythonMaxTemp")

#Create an RDD from the input data in HDFS
weatherData = sc.textFile(sys.argv[1], 1)

#Transform the data to extract/filter and then find the max temperature
max_temperature_per_year = weatherData.flatMap(extractData).reduceByKey(lambda a,b : a if int(a) > int(b) else b)

#Save the RDD back into HDFS
max_temperature_per_year.saveAsTextFile("output")
