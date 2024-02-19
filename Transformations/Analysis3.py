from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession

def crashesWhereAirbagsNotDeployed(spark : SparkSession):
    
    
    primaryPersonDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv", header = True, inferSchema = True).distinct()
    
    unitsUseDf = spark.read.csv(path = "../InputData/Units_use.csv", header = True, inferSchema = True).distinct()
    
    renamedUnitsUseDf = unitsUseDf.withColumnsRenamed(colsMap={"CRASH_ID":"crash_id_","UNIT_NBR":"unit_nbr_"})
    
    
    
    
    filteredPrimaryPersonDf = primaryPersonDf.filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("DEATH_CNT") == 1) & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED"))
    
    joinCondition = (renamedUnitsUseDf.crash_id_ == filteredPrimaryPersonDf.CRASH_ID) & (renamedUnitsUseDf.unit_nbr_ == filteredPrimaryPersonDf.UNIT_NBR)
    joinedDf = filteredPrimaryPersonDf.join(renamedUnitsUseDf, joinCondition ,"inner")
    joinedDf.printSchema()
    groupedByVehicleMakerDf = joinedDf.groupBy("VEH_MAKE_ID") \
                                      .agg(count_distinct("CRASH_ID").alias("TOTAL_ACCIDENTS")) \
                                      .orderBy(col("TOTAL_ACCIDENTS").desc()) \
                                      .limit(5)
                                      
    groupedByVehicleMakerDf.write.csv(path = "../OutputData/Analysis3/",mode = "overwrite", header = True)
    
    
    
if __name__  == "__main__":
    
    spark = createSparkSession()
    
    crashesWhereAirbagsNotDeployed(spark)
    
    spark.stop()