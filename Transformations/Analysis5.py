from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession


def stateWithHighestAccidents(spark : SparkSession):
    
    
    primaryPersonDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv" , header = True , inferSchema = True)
    
    highestStateDf = primaryPersonDf.filter(col("PRSN_GNDR_ID") != "FEMALE") \
                   .groupBy("DRVR_LIC_STATE_ID") \
                   .agg(count_distinct("CRASH_ID").alias("No_of_Accidents")) \
                   .orderBy(col("No_of_Accidents").desc()) \
                   .select(col("DRVR_LIC_STATE_ID")).limit(1)
    
    highestStateDf.write.csv(path = "../OutputData/Analysis5/",mode = "overwrite" , header = True)
    
    
    
if __name__ == "__main__":
    
    spark = createSparkSession()
    
    stateWithHighestAccidents(spark)
    
    spark.stop()