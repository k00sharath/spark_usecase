from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession

def findThirdtoFifthhighestvehicles(spark : SparkSession):
    
    unitsUseDf = spark.read.csv(path = "../InputData/Units_use.csv" , header = True , inferSchema = True).distinct()
    
    deathsByVehicleManufacturer = unitsUseDf.groupBy("VEH_MAKE_ID") \
                                            .agg(sum("TOT_INJRY_CNT").alias("INJURY_COUNT"),sum("DEATH_CNT").alias("DEATH_COUNT")) \
                                            .orderBy((col("INJURY_COUNT")+col("DEATH_COUNT")).desc()) \
                                            .limit(5) \
                                            .offset(2)
                                            
    deathsByVehicleManufacturer.write.csv(path = "../OutputData/Analysis6/",header = True,mode = "overwrite")
    
    
    
if __name__  == "__main__":
    
    spark = createSparkSession()
    
    findThirdtoFifthhighestvehicles(spark)
    
    spark.stop()

    
    