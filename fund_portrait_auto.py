from pyspark.sql import SparkSession
from fund_label import Fund_Label
import yaml


if __name__ == "__main__":
    print("This is a Spark Application")
    spark = SparkSession.builder.getOrCreate()

    master_filePath = "../data/master.csv"
    df_master = spark.read \
                       .format("csv") \
                       .option("header", True) \
                       .option("charset", "cp936") \
                       .load(master_filePath)

    with open('bound_portrait_conf.yaml', 'r', encoding='utf8') as f:
        mapping = yaml.load(f, yaml.Loader)

    risk_mapping = spark.createDataFrame(data=mapping["fund_risk_estimate"]["basic_risk_level"], schema = ["FundTypeName1", "FundTypeName2", "FundTypeName3", "RiskLevel"])
    f = Fund_Label()
    f.fund_risk_estimate(df_master, risk_mapping)