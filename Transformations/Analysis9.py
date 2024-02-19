from pyspark.sql.functions import *

from pyspark.sql import SparkSession

from session import createSparkSession



def findCrashesWithNoDamagedProperty(spark : SparkSession):
    
    damagePropertyDf = spark.read.csv(path = "../InputData/Damages_use.csv",header = True, inferSchema = True).distinct()
    
    crashIdsWithNoDamagedPropertyDf = damagePropertyDf.filter(col("DAMAGED_PROPERTY").isNull() | col("DAMAGED_PROPERTY").like("NONE%"))
    
    unitsUseDf = spark.read.csv(path = "../InputData/Units_use.csv", header = True , inferSchema = True).distinct()
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    
    spark = createSparkSession()
    
    findCrashesWithNoDamagedProperty(spark)
    
    spark.stop()  