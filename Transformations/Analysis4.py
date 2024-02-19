from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession


def findVehiclesWithValidLicenseInCrash(spark : SparkSession):
    
    primaryPersonDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv", header = True, inferSchema = True).distinct()
    
    unitsDf = spark.read.csv(path = "../InputData/Units_use.csv", header = True , inferSchema = True)
    
    hitandrunDf = unitsDf.filter(col("VEH_HNR_FL") == "Y").distinct() 


    renamedhitandrunDf = hitandrunDf.withColumnsRenamed(colsMap = {"CRASH_ID":"crash_id","UNIT_NBR":"unit_nbr"})
    joinedDf = primaryPersonDf \
    .join(renamedhitandrunDf , (primaryPersonDf.CRASH_ID == renamedhitandrunDf.crash_id) & (primaryPersonDf.UNIT_NBR == renamedhitandrunDf.unit_nbr) ,"inner") \
    .filter(primaryPersonDf.DRVR_LIC_CLS_ID.startswith("CLASS"))
    
    countDf = joinedDf.select(count_distinct("VIN").alias("VEHICLE_COUNT"))
    
    
    countDf.write.csv(path = "../OutputData/Analysis4/",mode = "overwrite",header = True)
    

if __name__ == "__main__":
    
    spark = createSparkSession()
    
    findVehiclesWithValidLicenseInCrash(spark)
    
    spark.stop()