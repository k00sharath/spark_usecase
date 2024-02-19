from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession

def findNoOfCrashes(spark : SparkSession):
    
    crashDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv", header = True, inferSchema = True)
    
    noOfMalesDied = crashDf.groupBy("CRASH_ID","PRSN_GNDR_ID") \
                           .agg(sum("DEATH_CNT").alias("totalDeaths")) \
                           .filter(col("PRSN_GNDR_ID") == "MALE") \
                           .filter(col("totalDeaths") > 2).select(count_distinct("CRASH_ID").alias("noOfCrashIds"))
    
    noOfMalesDied.write.csv(path = "../OutputData/Analysis1/", header = True, mode = "overwrite")


if __name__  == "__main__":
    
    spark = createSparkSession()
    
    findNoOfCrashes(spark)
    
    spark.stop()