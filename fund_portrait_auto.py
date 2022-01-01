from pyspark.sql import SparkSession
from pyspark.sql.types import *
from fund_label import Fund_Label
import yaml

fundarchives_filePath = "../data/fundarchives.csv"
secumain_filePath = "../data/secumain.csv"
fundtype_filePath = "../data/fundtype.csv"
fundrisklevel_filePath = "../data/fundrisklevel.csv"
fundtypechangenew_filePath = "../data/fundtypechangenew.csv"

fundarchives_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("MainCode", StringType(), False),
    StructField("SecurityCode", StringType(), False),
    StructField("ApplyingCodeFront", StringType(), False),
    StructField("ApplyingCodeBack", StringType(), False),
    StructField("EstablishmentDate",  DateType(), False),
    StructField("EstablishmentDateII",  DateType(), False),
    StructField("ShareProperties", IntegerType(), False),
    StructField("ExpireDate",  DateType(), False)
])

secumain_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("ChiName", StringType(), False),
    StructField("SecuAbbr", StringType(), False),
    StructField("SecuCategory", IntegerType(), False),
    StructField("ListedState", IntegerType(), False)
])

fundtype_schema = StructType([
    StructField("FundTypeCode", IntegerType(), True),
    StructField("FundTypeName", StringType(), False),
    StructField("Level", IntegerType(), False),
    StructField("IfExecuted", IntegerType(), False)
])

fundrisklevel_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("RiskLevel", IntegerType(), False),
    StructField("BeginDate", DateType(), False)
])

fundtypechangenew_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("FundType", IntegerType(), False),
    StructField("StartDate", DateType(), False)
])

if __name__ == "__main__":
    print("This is a Spark Application")
    spark = SparkSession.builder.getOrCreate()

    df_fundarchives = spark.read.format("csv").option("header", True).schema(fundarchives_schema).load(fundarchives_filePath)
    df_secumain = spark.read.format("csv").option("header", True).option("charset", "cp936").schema(secumain_schema).load(secumain_filePath)
    df_fundtype = spark.read.format("csv").option("header", True).option("charset", "cp936").schema(fundtype_schema).load(fundtype_filePath)
    df_fundrisklevel = spark.read.format("csv").option("header", True).load(fundrisklevel_filePath)
    df_fundtypechangenew = spark.read.format("csv").option("header", True).schema(fundtypechangenew_schema).load(fundtypechangenew_filePath)
    
    with open('bound_portrait_conf.yaml', 'r', encoding='utf8') as f:
        mapping = yaml.load(f, yaml.Loader)

    risk_mapping = spark.createDataFrame(data=mapping["fund_risk_estimate"]["basic_risk_level"], schema = ["FundTypeName1", "FundTypeName2", "FundTypeName3", "RiskLevel"])

    
    f = Fund_Label()
    f.fund_risk_estimate(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping)