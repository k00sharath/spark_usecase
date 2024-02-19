from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from pyspark.sql import Window

from session import createSparkSession

def topEthnicityByVehicleClass(spark : SparkSession):
    
    primaryPersonDf = spark.read.csv(path = "../InputData/Primary_Person_use.csv", header = True, inferSchema = True).distinct()
    
    unitsUseDf = spark.read.csv(path = "../InputData/Units_use.csv", header = True , inferSchema = True).distinct()
    
    
    renamedPrimaryPersonDf = primaryPersonDf.withColumnsRenamed({"CRASH_ID":"crash_id","UNIT_NBR":"unit_nbr"})
    
    
    joinedDf = renamedPrimaryPersonDf.join(unitsUseDf,(renamedPrimaryPersonDf.crash_id == unitsUseDf.CRASH_ID) & (renamedPrimaryPersonDf.unit_nbr == unitsUseDf.UNIT_NBR),"inner")

    groupedByVehClassDf = joinedDf.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").agg(count("*").alias("COUNT_BY_VEH_STYL"))
    
    colList = ["VEH_BODY_STYL_ID"]
    
    windowByVehicleBodyStyleAndEthinicity = Window.partitionBy(colList).orderBy(col("COUNT_BY_VEH_STYL").desc())
    
    topEthicGroupByVehicleClassDf = groupedByVehClassDf.withColumn("rank",row_number().over(windowByVehicleBodyStyleAndEthinicity)) \
                                                       .filter(col("rank") <= 1) \
                                                       .drop("rank")
                                                       
                                                    
    topEthicGroupByVehicleClassDf.write.csv(path = "../OutputData/Analysis7/", mode = "overwrite", header = True)
    
    



if __name__ == "__main__":
    
    spark = createSparkSession()
    
    topEthnicityByVehicleClass(spark)
    
    spark.stop()