from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession


def findTop5ZipCodes(spark : SparkSession):
    
     primaryPersonDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv", header = True, inferSchema = True).distinct()
     
     groupedByZipCodeDf = primaryPersonDf.filter(col("PRSN_ALC_RSLT_ID") == "Positive") \
                    .groupBy("DRVR_ZIP") \
                    .agg(count_distinct("CRASH_ID").alias("TOTAL_ACCIDENTS")) \
                    .orderBy(col("TOTAL_ACCIDENTS").desc()).limit(5)
                    
     groupedByZipCodeDf.write.csv(path = "../OutputData/Analysis8/", mode = "overwrite" , header = True)
                    
     
if __name__ == "__main__":
    
    spark = createSparkSession()
    
    findTop5ZipCodes(spark)
    
    spark.stop()  