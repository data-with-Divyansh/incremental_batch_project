import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

os.environ['PYSPARK_PYTHON'] = r"C:\Users\PCNAME\AppData\Local\Programs\Python\Python310\python.exe"
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("====================== pyspark started ==================")
print()

# Simple read first batch data and add respective columns ( )
###################  FIRST BATCH #######################
wn = Window.orderBy("custid")
day1df = (spark.read.csv("batch1.csv", sep=",", header="true")
          .withColumn("deleteind",lit("0"))
          .withColumn("incid",row_number().over(wn))
          .withColumn("batchid",lit(0))
          )
day1df.show()

#day3df.write.format("csv").mode("append").save(output/batch0)

################### SECOND BATCH ##########################
# Read second batch data and add deleteind column with all values as 0 initially

df2 = (spark.read.csv("batch2.csv",sep=",",header="true")
       .select("custid","permanentAddress","temporaryAddress","products")
       .withColumn("deleteind",lit(0))
       )
df2.show()

# Perform a left_anti join with yesterday's final data to get deleted records
joindf = (day1df.join(df2, "custid", "left_anti").drop("incid", "batchid")
          .withColumn("deleteind",lit(1))
          .union(df2)
          )
joindf.show()

maxincid = day1df.select(max("incid")).collect()[0][0]  # Fetch yesterday's max increment id (.collect()[0][0] will collect the dataframe value as string for further use)
#print(maxincid) # To verify
maxbatchid = day1df.select(max("batchid")).collect()[0][0] # Fetch yesterday's batch id
#print(maxbatchid) # To verify

# Add incid and batchid to today's batch data incrementally using yesterday's maxincid and maxbatchid

day2df = joindf.rdd.zipWithIndex().map(
    lambda row_index:row_index[0] + (row_index[1] + maxincid+1,)
).toDF(joindf.columns + ["incid"]).withColumn("batchid",lit(maxbatchid+1))
day2df.show()

#day3df.write.format("csv").mode("append").save(output/batch1)

####################### THIRD BATCH ##########################

df3 = (spark.read.csv("batch3.csv", sep=",", header="true")
       .select("custid","permanentAddress","temporaryAddress","products")
       .withColumn("deleteind",lit(0))
       )
df3.show()

joindf2 = (day2df.join(df3,"custid","left_anti").drop("incid","batchid")
           .withColumn("deleteind",lit(1))
           .union(df3)
           )
joindf2.show()

day2maxincid = day2df.select(max("incid")).collect()[0][0]
print(day2maxincid)
day2maxbatchid = day2df.select(max("batchid")).collect()[0][0]
print(day2maxbatchid)

day3df = joindf2.rdd.zipWithIndex().map(
    lambda row_index:row_index[0] + (row_index[1] + day2maxincid+1,)
).toDF(joindf2.columns + ["incid"]).withColumn("batchid",lit(day2maxbatchid + 1))
day3df.show()

#day3df.write.format("csv").mode("append").save(output/batch2)


