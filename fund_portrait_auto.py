from pyspark.sql import SparkSession
from pyspark.sql.types import *
from fund_label import Fund_Label
from pyspark.sql.functions import *
import yaml

fundarchives_filePath = "../data/fundarchives.csv"
secumain_filePath = "../data/secumain.csv"
fundtype_filePath = "../data/fundtype.csv"
fundrisklevel_filePath = "../data/fundrisklevel.csv"
fundtypechangenew_filePath = "../data/fundtypechangenew.csv"
mainfinancialindex_filePath = "../data/mainfinancialindex.csv"
last_qrt_filePath = "../data/基金风险等级三季度.csv"
unitnvrestored_filePath = "../data/unitnvrestored.csv"
zzqz_filePath = "../data/zzqz.csv"
zzzcf_filePath = "../data/zzzcf.csv"

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
    StructField("FNodeCode", IntegerType(), False),
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

mainfinancialindex_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("EndDate", DateType(), False),
    StructField("NetAssetsValue", DecimalType(18,3), False)
])

last_qrt_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("FundSizeStatus", IntegerType(), False),
    StructField("SDStatus", IntegerType(), False),
    StructField("HigherRiskLevel", IntegerType(), False),
    StructField("HigherRiskLevelName", StringType(), False)
])

unitnvrestored_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("TradingDay", DateType(), False),
    StructField("UnitNVRestored", DecimalType(18,6), False),
    StructField("NVRDailyGrowthRate", DecimalType(18,8), False)
])

zzqz_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("TradingDay", DateType(), False),
    StructField("ClosePrice", DecimalType(18,4), False),
    StructField("ChangePCT", DecimalType(18,8), False)
])

zzzcf_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("TradingDay", DateType(), False),
    StructField("ClosePrice", DecimalType(18,4), False),
    StructField("ChangePCT", DecimalType(18,8), False)
])

if __name__ == "__main__":
    print("This is a Spark Application")
    spark = SparkSession.builder.master("local[4]")\
                        .appName('FundLabel')\
                        .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df_fundarchives = spark.read.format("csv").option("header", False).schema(fundarchives_schema).load(fundarchives_filePath).select("InnerCode", "MainCode", "SecurityCode", "ApplyingCodeFront", "ApplyingCodeBack", "EstablishmentDate", "EstablishmentDateII", "ShareProperties", "ExpireDate")
    df_secumain = spark.read.format("csv").option("header", False).schema(secumain_schema).load(secumain_filePath).select("InnerCode", "ChiName", "SecuAbbr", "SecuCategory", "ListedState")
    df_fundtype = spark.read.format("csv").option("header", True).option("charset", "cp936").schema(fundtype_schema).load(fundtype_filePath).select("FundTypeCode", "FundTypeName", "FNodeCode", "Level", "IfExecuted")
    df_fundrisklevel = spark.read.format("csv").option("header", True).load(fundrisklevel_filePath).select("InnerCode", "RiskLevel", "BeginDate")
    df_fundrisklevel = df_fundrisklevel.withColumn("BeginDate", to_date("BeginDate", "yyyy-MM-dd"))
    df_fundtypechangenew = spark.read.format("csv").option("header", False).schema(fundtypechangenew_schema).load(fundtypechangenew_filePath).select("InnerCode", "FundType", "StartDate")
    df_mainfinancialindex = spark.read.format("csv").option("header", False).schema(mainfinancialindex_schema).load(mainfinancialindex_filePath).select("InnerCode", "EndDate", "NetAssetsValue")
    df_last_qrt = spark.read.format("csv").option("header", True).option("charset", "cp936").schema(last_qrt_schema).load(last_qrt_filePath).select("InnerCode", "FundSizeStatus", "SDStatus", "HigherRiskLevel", "HigherRiskLevelName")
    df_unitnvrestored = spark.read.format("csv").option("header", True).schema(unitnvrestored_schema).load(unitnvrestored_filePath).select("InnerCode", "TradingDay", "UnitNVRestored", "NVRDailyGrowthRate")
    df_zzqz = spark.read.format("csv").option("header", True).schema(zzqz_schema).load(zzqz_filePath).select("InnerCode", "TradingDay", "ClosePrice", "ChangePCT")
    df_zzzcf = spark.read.format("csv").option("header", True).schema(zzzcf_schema).load(zzzcf_filePath).select("InnerCode", "TradingDay", "ClosePrice", "ChangePCT")
    df_mainfinancialindex = df_mainfinancialindex.join(df_fundarchives.select("InnerCode", "MainCode"), ["InnerCode"], "left")
    
    with open('bound_portrait_conf.yaml', 'r', encoding='utf8') as f:
        mapping = yaml.load(f, yaml.Loader)

    risk_mapping = spark.createDataFrame(data=mapping["fund_risk_estimate"]["basic_risk_level"], schema = ["FundTypeName1", "FundTypeName2", "FundTypeName3", "RiskLevel"])

    
    f = Fund_Label()
    f.fund_risk_estimate(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping, df_mainfinancialindex, df_last_qrt, df_unitnvrestored, df_zzqz, df_zzzcf)