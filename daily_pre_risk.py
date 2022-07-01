from pyspark.sql import SparkSession
from pyspark.sql.types import *
from fund_label import Fund_Label
from pyspark.sql.functions import *
import yaml

folder = "data_pre"
fundarchives_filePath = "../{}/fundarchives.csv".format(folder)
secumain_filePath = "../{}/secumain.csv".format(folder)
fundtype_filePath = "../{}/fundtype.csv".format(folder)
fundrisklevel_filePath = "../{}/fundrisklevel.csv".format(folder)
fundtypechangenew_filePath = "../{}/fundtypechangenew.csv".format(folder)
last_qrt_filePath = "../{}/last_qrt.csv".format(folder)
fundtyperisk_filePath = "../{}/fundtyperisklevel.csv".format(folder)

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
    StructField("InfoSource", StringType(), False),
    StructField("RiskLevel", IntegerType(), False),
    StructField("BeginDate", DateType(), False),
    StructField("IfEffected", IntegerType(), False)
])

fundtypechangenew_schema = StructType([
    StructField("InnerCode", IntegerType(), True),
    StructField("FundType", IntegerType(), False),
    StructField("StartDate", DateType(), False)
])


last_qrt_schema = StructType([
    StructField("EndDate", DateType(), False),
    StructField("SecurityCode", StringType(), False),
    StructField("Number", IntegerType(), False)
])

fundtyperisk_schema = StructType([
    StructField("FundTypeCode", StringType(), False),
    StructField("FundTypeName", StringType(), False)
])

if __name__ == "__main__":
    print("每日新增fund事前计算")
    spark = SparkSession.builder.master("local[4]")\
                        .appName('FundLabel')\
                        .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df_fundarchives = spark.read.format("csv").option("header", False).schema(fundarchives_schema).load(fundarchives_filePath).select("InnerCode", "MainCode", "SecurityCode", "ApplyingCodeFront", "ApplyingCodeBack", "EstablishmentDate", "EstablishmentDateII", "ShareProperties", "ExpireDate")
    df_secumain = spark.read.format("csv").option("header", False).schema(secumain_schema).load(secumain_filePath).select("InnerCode", "ChiName", "SecuAbbr", "SecuCategory", "ListedState")
    df_fundtype = spark.read.format("csv").option("header", True).option("charset", "cp936").schema(fundtype_schema).load(fundtype_filePath).select("FundTypeCode", "FundTypeName", "FNodeCode", "Level", "IfExecuted")
    df_fundrisklevel = spark.read.format("csv").option("header", True).load(fundrisklevel_filePath).select("InnerCode", "InfoSource", "RiskLevel", "BeginDate", "IfEffected")
    df_fundtypechangenew = spark.read.format("csv").option("header", False).schema(fundtypechangenew_schema).load(fundtypechangenew_filePath).select("InnerCode", "FundType", "StartDate")
    df_last_qrt = spark.read.format("csv").option("header", True).schema(last_qrt_schema).load(last_qrt_filePath).select("SecurityCode", "EndDate", "Number")
    df_fundtyperisklevel = spark.read.format("csv").option("header", True).schema(fundtyperisk_schema).load(fundtyperisk_filePath).select("FundTypeCode", 'FundTypeName')

    with open('bound_portrait_conf.yaml', 'r', encoding='utf8') as f:
        mapping = yaml.load(f, yaml.Loader)

    risk_mapping = spark.createDataFrame(data=mapping["fund_risk_estimate"]["basic_risk_level"], schema = ["FundTypeName1", "FundTypeName2", "FundTypeName3", "InitialRiskLevel"])

    
    f = Fund_Label()
    f.daily_new_fund_risk(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping, df_fundtyperisklevel, df_last_qrt)