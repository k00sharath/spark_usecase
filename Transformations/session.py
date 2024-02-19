from pyspark.sql import SparkSession

from pyspark import SparkConf

def createSparkSession():
    
    sparkConfig = SparkConf() 
    
    sparkConfig.set("spark.app.name","crashAnalysis") 
    sparkConfig.set("spark.master","local[*]")
    sparkConfig.set("spark.sql.caseSensitive",True)
                  
    spark = SparkSession.builder.config(conf = sparkConfig).getOrCreate()

    return spark




    #
