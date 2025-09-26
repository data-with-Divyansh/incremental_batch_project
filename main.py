import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\Computername\.jdks\corretto-1.8.0_462'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("====================== pyspark started ==================")
print()

###################  FIRST BATCH #######################
wn = Window.orderBy("custid")
df = (spark.read.csv("batch1.csv",sep=",",header="true")
      .withColumn("deleteind",lit("0"))
      .withColumn("incid",row_number().over(wn))
      .withColumn("batchid",lit(0))
      )
df.show()

################### SECOND BATCH ##########################

df2 = (spark.read.csv("batch2.csv",sep=",",header="true")
       .select("custid","permanentAddress","temporaryAddress","products")
       .withColumn("deleteind",lit(0))
       )
df2.show()

joindf = (df.join(df2,"custid","left_anti").drop("incid","batchid")
          .withColumn("deleteind",lit(1))
          .union(df2)
          )
joindf.show()

maxincid = df.select(max("incid")).collect()[0][0]
print(maxincid)
maxbatchid = df.select(max("batchid")).collect()[0][0]
print(maxbatchid)

fdf = joindf.rdd.zipWithIndex().map(
    lambda row_index:row_index[0] + (row_index[1] + maxincid+1,)
).toDF(joindf.columns + ["incid"]).withColumn("batchid",lit(maxbatchid+1))
fdf.show()

#fdf.write.format(sql/snowflake/s3).mode("append").save(targetlocation)


