from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import datetime
from pyspark.sql.types import *

@udf(returnType=StringType())
def cal_risk(ChiName, FundTypeCode1, FundTypeCode2, RiskLevel, ShareProperties):
    """
    重新定义一般基金
    检查ChiName, FundTypeCode1, FundTypeCode2字段, 根据搜索关键字
    覆盖原本的风险等级
    """
    # 所有FOF基金都为R3
    if FundTypeCode1 == 15:
        return "R3"

        # QDII基金
    elif FundTypeCode1 == 14 and FundTypeCode2 == 1450:
        if ChiName is None:
            return RiskLevel
        # 商品QDII基金（除黄金QDII）
        elif "原油" in ChiName or "商品" in ChiName or "通胀" in ChiName:
            return "R5-QDII基金-商品QDII基金-商品QDII基金（除黄金QDII）"
        # 黄金QDII基金
        elif "黄金" in ChiName:
            return "R3-QDII基金-商品QDII基金-黄金QDII基金"
        else:
            return RiskLevel
    
    # 商品基金新分类    
    elif FundTypeCode1 == 17:
        if ChiName is None:
            return RiskLevel
        elif "期货" in ChiName:
            return "R5-商品基金-商品基金-商品基金（除黄金基金）"
        elif "黄金" in ChiName or "上海金" in ChiName:
            return "R3-商品基金-商品基金-黄金基金"
        else:
            return RiskLevel

    # 债券基金新分类
    elif FundTypeCode1 == 12:
        if ChiName is None:
            return RiskLevel
        elif "可转换债券" in ChiName and "分级" in ChiName and ShareProperties == 1:
            return "R3-债券基金-债券分级子基金-可转债分级子基金（优先份额）"

        elif "可转换债券" in ChiName and "分级" in ChiName and ShareProperties == 2:
            return "R5-债券基金-债券分级子基金-可转债分级子基金（进取份额）"

        elif "可转换债券" not in ChiName and "分级" in ChiName and ShareProperties == 1:
            return "R3-债券基金-债券分级子基金-普通债券分级子基金（优先份额）"
        
        elif "可转换债券" not in ChiName and "分级" in ChiName and ShareProperties == 2:
            return "R4-债券基金-债券分级子基金-普通债券分级子基金（进取份额）"

        elif FundTypeCode2 == 1220 and "可转换债券" in ChiName:
            return "R3-债券基金-指数型债券基金-可转债指数基金"
        else:
            return RiskLevel

    # 不是新分类保持配置中的分类
    else:
        return RiskLevel

@udf(returnType=StringType())
def cal_speical_fund_risk(ChiName, SecurityCode, InitRiskLevel):
    # 新三板基金SecurityCode
    neeq_r3_list = ["009697", "009698", "009693", "009688", "009681", "009682", "009683", "009684", "009695", "009696", "009867", "009868", 
                    "010887", "010888", "012107", "012108", "011886", "011887", "011011", "010646", "010647", "012850", "012851"]
    neeq_r4_list = ["011783", "011530", "910006", "910009", "014269" , "014270", "014271", "014272", "014273", "014274", "014275", "014276", 
                    "014277", "014278", "014279", "014280", "014283", "014294", "014062", "014063", "014185", "014186", "014232", "014233"]
    
    if not ChiName: # ChinName有空白
        return InitRiskLevel
    elif "科创" in ChiName or "科技创新" in ChiName:
        return "R4"
    elif "REIT" in ChiName.upper():
        return "R4"
    elif SecurityCode in neeq_r3_list:
        return "R3"
    elif SecurityCode in neeq_r4_list:
        return "R4"
    else:
        return InitRiskLevel

@udf(returnType=StringType())
def tranform_text(PreviousFundTypeName3):
    if PreviousFundTypeName3:
        return ("由%s转型而来")%(PreviousFundTypeName3)
    else:
        return 

@udf(returnType=IntegerType())
def net_asset_flag(ForthQrtNV, ThirdQrtNV, SecondQrtNV, LastQrtNV):
    try:
        if LastQrtNV < 1000:
            return 1
        elif LastQrtNV < 5000 and SecondQrtNV < 5000 and ThirdQrtNV < 5000 and ForthQrtNV < 5000:
            return 1
        elif LastQrtNV > 5000 and SecondQrtNV > 5000 and ThirdQrtNV > 5000 and ForthQrtNV > 5000:
            return 2
        else:
            return 0
    except:
        return 0


@udf(returnType=StringType())
def final_risk(InitRiskLevel, EstablishmentLength, NetAssetFlag, FundSizeStatus, SDStatus, SDFlag):
    # 如果基金成立未满1.5年，基础风险等级及为风险等级
    if EstablishmentLength < 18:
        return InitRiskLevel
    # 如果基金成立满1.5年但小于3.5年
    elif EstablishmentLength >= 18 and EstablishmentLength <= 42:
        # 如果之前被调整过并且触发回调条件
        if FundSizeStatus == 1:
            if NetAssetFlag == 2:
                return InitRiskLevel + "-规模上升-0-0"
            else:
                risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
                return "R" + str(risk_level)
        # 如果之前没被调整过并且触发上调条件
        elif FundSizeStatus == 0 and NetAssetFlag == 1:
            risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
            return "R"+ str(risk_level) + "-规模下降-1-0"
        else:
            return InitRiskLevel
    elif EstablishmentLength > 42:
        # 如果之前被调整过并且触发回调条件
        if FundSizeStatus == 1 or SDStatus == 1: 
            if NetAssetFlag == 2 and InitRiskLevel in ["R2", "R3", "R4"] and SDFlag == 2:
                return InitRiskLevel + "-规模上升,波动率下降-0-0"
            else:
                risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
                return "R" + str(risk_level)
        # 如果之前没被调整过并触发两个上调条件
        elif FundSizeStatus == 0 and SDStatus == 0 and NetAssetFlag == 1 and InitRiskLevel in ["R2", "R3", "R4"] and SDFlag == 1:
            risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
            return "R"+ str(risk_level) + "-规模下降,波动率上升-1-1"
        # 如果之前没被调整过并触发规模上调条件
        elif FundSizeStatus == 0 and SDStatus == 0 and NetAssetFlag == 1:
            risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
            return "R"+ str(risk_level) + "-规模下降-1-0"
        # 如果之前没被调整过并触发规模波动条件
        elif FundSizeStatus == 0 and SDStatus == 0 and InitRiskLevel in ["R2", "R3", "R4"] and SDFlag == 1:
            risk_level = (int(InitRiskLevel[-1] )+ 1) if (int(InitRiskLevel[-1]) + 1) < 5 else 5
            return "R"+ str(risk_level) + "-波动率上升-0-1"
        else:
            return InitRiskLevel


@udf(returnType=DecimalType(18,6))
def volatility_calculation(SubProduct):
    result = 1
    for product in SubProduct:
        result *= product
    return result - 1


@udf(returnType=IntegerType())
def sd_flag(LastQrtSD, SecondQrtSD, ThirdQrtSD, ForthQrtSD, LastQrtStandard, SecondQrtStandard, ThirdQrtStandard, ForthQrtStandard):
    try:
        if LastQrtSD > LastQrtStandard and SecondQrtSD > SecondQrtStandard and ThirdQrtSD > ThirdQrtStandard and ForthQrtSD > ForthQrtStandard:
            return 1
        elif LastQrtSD <= LastQrtStandard and SecondQrtSD <= SecondQrtStandard and ThirdQrtSD <= ThirdQrtStandard and ForthQrtSD <= ForthQrtStandard:
            return 2
        else:
            return 0
    except:
        return 0


@udf(returnType=StringType())
def level_mapping(level):
    if level is None:
        return "未知"
    mapping = {
        "R1": "低",
        "R2": "较低",
        "R3": "中等",
        "R4": "较高",
        "R5": "高"
    }
    return mapping[level]


def aggVolatility(df, quarter, columnName): 
    if quarter == 4:
        aliasColumn = "LastQrtSD"
    elif quarter == 3:
        aliasColumn = "SecondQrtSD"
    elif quarter == 2:
        aliasColumn = "ThirdQrtSD"
    elif quarter == 1:
        aliasColumn = "ForthQrtSD"
        
    df = df.withColumn("weekNum", floor(datediff(col("TradingDay"), lit("2018-02-26"))/7)) \
                 .withColumn("NVRDailyGrowthRatePlus", col(columnName)/100 + 1)
    df = df.groupBy("InnerCode", "weekNum").agg(collect_list("NVRDailyGrowthRatePlus").alias("SubProduct"))
    df = df.withColumn("VolatilityByWeek", volatility_calculation("SubProduct"))
    df = df.groupBy("InnerCode").agg(stddev("VolatilityByWeek").alias(aliasColumn))
    
    return df


def volatilityByQuarter(df, date_threshod, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_6, date_threshod_7, date_threshod_8, date_threshod_9, datasetName):
    if datasetName == "unitnvrestored":
        columnName = "NVRDailyGrowthRate"
    else:
        columnName = "ChangePCT"
        
    df_q4 = df.filter((col("TradingDay") > date_threshod_6) & (col("TradingDay") <= date_threshod))
    df_q3 = df.filter((col("TradingDay") > date_threshod_7) & (col("TradingDay") <= date_threshod_2))
    df_q2 = df.filter((col("TradingDay") > date_threshod_8) & (col("TradingDay") <= date_threshod_3))
    df_q1 = df.filter((col("TradingDay") > date_threshod_9) & (col("TradingDay") <= date_threshod_4))
    
    df_q4 = aggVolatility(df_q4, 4, columnName)
    df_q3 = aggVolatility(df_q3, 3, columnName)
    df_q2 = aggVolatility(df_q2, 2, columnName)
    df_q1 = aggVolatility(df_q1, 1, columnName)
    
    df = df_q4.join(df_q3, ["InnerCode"], "inner") \
              .join(df_q2, ["InnerCode"], "inner") \
              .join(df_q1, ["InnerCode"], "inner")
    
    return df


def previous_quarter(ref):
    if ref.month < 4:
        return datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 9, 30).strftime('%Y-%m-%d'), datetime(ref.year-1, 6, 30).strftime('%Y-%m-%d'), datetime(ref.year-1, 3, 31).strftime('%Y-%m-%d')
    elif ref.month < 7:
        return datetime(ref.year, 3, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 9, 30).strftime('%Y-%m-%d'), datetime(ref.year-1, 6, 30).strftime('%Y-%m-%d')
    elif ref.month < 10:
        return datetime(ref.year, 6, 30).strftime('%Y-%m-%d'), datetime(ref.year, 3, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 9, 30).strftime('%Y-%m-%d')
    return datetime(ref.year, 9, 30).strftime('%Y-%m-%d'), datetime(ref.year, 6, 30).strftime('%Y-%m-%d'), datetime(ref.year, 3, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d')


def pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, date_threshod):
    """
    根据query来整合基金的type和名称
    """

    # SecuCategory IN (8,13) and ListedState IN (1,9)
    df_secumain = df_secumain.filter(((col("SecuCategory") == 8) | (col("SecuCategory") == 13)))

    # dbo.MFE_FundTypeChangeNew WHERE StartDate <= date_threshod AND FundType NOT LIKE '18%'
    df_fundtypechangenew = df_fundtypechangenew.filter((col("StartDate") <= date_threshod) & (~(col("FundType").startswith("18"))))
    windowSpec = Window.partitionBy("InnerCode").orderBy(desc("StartDate"))
    df_fundtypechangenew = df_fundtypechangenew.withColumn("row_num", row_number().over(windowSpec))
    df_fundtypechangenew = df_fundtypechangenew.filter(col("row_num") == 1).select(col("InnerCode"), col("FundType").alias("FundTypeCode3"))

    # fund type name by joining three levels
    df_fundtype_level_1 = df_fundtype.filter((col("Level") == 1) & (col("IfExecuted") == 1))
    df_fundtype_level_2 = df_fundtype.filter((col("Level") == 2) & (col("IfExecuted") == 1))
    df_fundtype_level_3 = df_fundtype.filter((col("Level") == 3) & (col("IfExecuted") == 1))
    df_fundtype = df_fundtype_level_3.alias("l3").join(df_fundtype_level_2.alias("l2"), col("l2.FundTypeCode") == col("l3.FNodeCode"), "left") \
                                                 .join(df_fundtype_level_1.alias("l1"),col("l1.FundTypeCode") == col("l2.FNodeCode"), "left") \
                                                 .select(col("l1.FundTypeCode").alias("FundTypeCode1"),
                                                         col("l1.FundTypeName").alias("FundTypeName1"),
                                                         col("l2.FundTypeCode").alias("FundTypeCode2"),
                                                         col("l2.FundTypeName").alias("FundTypeName2"),
                                                         col("l3.FundTypeCode").alias("FundTypeCode3"),
                                                         col("l3.FundTypeName").alias("FundTypeName3"))
                                    
    # WHERE BeginDate <= date_threshod
    df_fundrisklevel = df_fundrisklevel.filter(col("BeginDate") <= date_threshod)
    windowSpec = Window.partitionBy("InnerCode").orderBy(desc("BeginDate"))
    df_fundrisklevel = df_fundrisklevel.withColumn("row_num", row_number().over(windowSpec))\
                                       .withColumn("OfficialRiskLevel", when(col("RiskLevel") == 1, "R1")
                                                                       .when(col("RiskLevel") == 2, "R2")
                                                                       .when(col("RiskLevel") == 3, "R3")
                                                                       .when(col("RiskLevel") == 4, "R4")
                                                                       .when(col("RiskLevel") == 5, "R5")
                                                                       .otherwise(None))

    df_fundrisklevel = df_fundrisklevel.filter(col("row_num") == 1) \
                                       .select(col("InnerCode"), 
                                               col("OfficialRiskLevel"))

    df_master = df_fundarchives.join(df_secumain, ["InnerCode"], "left") \
                               .join(df_fundtypechangenew, ["InnerCode"], "left") \
                               .join(df_fundtype, ["FundTypeCode3"], "left") \
                               .join(df_fundrisklevel, ["InnerCode"], "left")

    return df_master


def pre_fund_risk_calc(df_master, risk_mapping, date_threshod, date_threshod_2):
    """
    基金事前风险计算
    """
    # 通过risk配置表中确定新的分类
    df_master = df_master.join(risk_mapping, ["FundTypeName1", "FundTypeName2", "FundTypeName3"], "left")
    # 一般基金计算
    df_master = df_master.withColumn("InitRiskLevel", cal_risk("ChiName", "FundTypeCode1", "FundTypeCode2", "RiskLevel", "ShareProperties"))
    # 重新定义1,2,3级分类名称
    df_master = df_master.withColumn("FundTypeName3", when(col("InitRiskLevel").contains("-"), split(col("InitRiskLevel"), "-").getItem(3))
                                                     .otherwise(col("FundTypeName3")))\
                         .withColumn("FundTypeName2", when(col("InitRiskLevel").contains("-"), split(col("InitRiskLevel"), "-").getItem(2))
                                                     .otherwise(col("FundTypeName2")))\
                         .withColumn("FundTypeName1", when(col("InitRiskLevel").contains("-"), split(col("InitRiskLevel"), "-").getItem(1))
                                                     .otherwise(col("FundTypeName1")))\
                         .withColumn("InitRiskLevel", when(col("InitRiskLevel").contains("-"), split(col("InitRiskLevel"), "-").getItem(0))
                                                     .otherwise(col("InitRiskLevel")))\
    # 将2级名称中的指数替换成指数型，债券基金替换成债券基金（不含封闭式） 
    df_master = df_master.withColumn("FundTypeName2", when(col("FundTypeName2").contains("指数型"), regexp_replace(df_master.FundTypeName2,'指数型','指数'))
                                                     .otherwise(col("FundTypeName2")))\
                         .withColumn("FundTypeName1", when(col("FundTypeName1").contains("债券基金"), regexp_replace(df_master.FundTypeName1,'债券基金','债券基金（不含封闭式）'))
                                                     .otherwise(col("FundTypeName1")))

    # 根据主代码找转型基金
    df_master_J = df_master.filter((col("MainCode").endswith("J")) & (col("ExpireDate") > date_threshod_2) & (col("MainCode") == col("SecurityCode")))\
                           .select(col("FundTypeName3").alias("PreviousFundTypeName3"), 
                                   col("MainCode").alias("MainCodeJ"),
                                   col("ExpireDate").alias("ExpireDateJ"))
                                                                         
    df_master_J = df_master_J.withColumn("MainCodeJoin", substring("MainCodeJ", 0, 6))

    df_master = df_master.filter((col("EstablishmentDate") <= date_threshod) & ((col("ExpireDate") > date_threshod) | (col("ExpireDate").isNull())))
    # 特殊基金计算
    df_master = df_master.withColumn("InitRiskLevel", cal_speical_fund_risk("ChiName", "SecurityCode", "InitRiskLevel"))

    # 根据主代码找转型基金
    df_master =  df_master.alias("dfm").join(df_master_J.alias("dfmj"), [col("dfm.MainCode") == col("dfmj.MainCodeJoin"),
                                                                         col("dfm.EstablishmentDate") == col("dfmj.ExpireDateJ"),
                                                                         col("dfm.EstablishmentDateII").isNotNull()], "left")

    df_master = df_master.withColumn("ChangeReason", tranform_text("PreviousFundTypeName3"))
    df_master = df_master.withColumn("FundTypeName2", when(col("FundTypeName2").contains("FOF"), regexp_replace(df_master.FundTypeName2,'FOF','股票FOF'))
                                                     .otherwise(col("FundTypeName2")))\
                         .withColumn("FundTypeName3", when(col("FundTypeName3").contains("FOF"), regexp_replace(df_master.FundTypeName1,'FOF','股票FOF'))
                                                     .otherwise(col("FundTypeName3")))\
                         .withColumn("SecurityCode", when(col("SecurityCode").contains("J"), regexp_replace(df_master.SecurityCode,'J',''))
                                                     .otherwise(col("SecurityCode")))

    return df_master

def post_fund_risk_calc(df_master, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_5, date_threshod, df_mainfinancialindex, df_last_qrt, df_unitnvrestored_master, zzzs_dict):
    """
    基金事后风险计算
    """
    # 选取过去一年的所有净资产
    df_mainfinancialindex = df_mainfinancialindex.filter((col("EndDate") > date_threshod_5) & (col("EndDate") <= date_threshod))
    df_mainfinancialindex = df_mainfinancialindex.withColumn("NetAssetsValue", col("NetAssetsValue")/10000) \
                                                 .withColumn("Qrt", when(col("EndDate") > date_threshod_2, "LastQrtNV")
                                                                   .when(col("EndDate") > date_threshod_3, "SecondQrtNV")
                                                                   .when(col("EndDate") > date_threshod_4, "ThirdQrtNV")
                                                                   .otherwise("ForthQrtNV"))

    df_mainfinancialindex = df_mainfinancialindex.groupBy("MainCode", "Qrt") \
                                                 .agg(sum("NetAssetsValue").alias("NetAssetsValueSum"))
    df_mainfinancialindex = df_mainfinancialindex.groupBy("MainCode").pivot("Qrt").sum("NetAssetsValueSum")

    # 将过去四个季度的净资产join到master table
    df_master = df_master.join(df_mainfinancialindex, ["MainCode"], "left")
    df_master = df_master.withColumn("EstablishmentLength", months_between(to_date(lit(date_threshod)), col("EstablishmentDate")))
    df_master = df_master.withColumn("NetAssetFlag", net_asset_flag("ForthQrtNV", "ThirdQrtNV", "SecondQrtNV", "LastQrtNV"))

    # 调取上一季度的结果
    df_master = df_master.join(df_last_qrt, ["InnerCode"], "left")
    df_master = df_master.join(df_unitnvrestored_master, ["InnerCode"], "left")

    # 计算波动率倍数
    df_master = df_master.withColumn("LastQrtSDFactor",  when(col("InitRiskLevel") == "R2", col("LastQrtSD") / (0.8 * zzzs_dict[2968][0] + 0.2 * zzzs_dict[14110][0]))
                                                        .when(col("InitRiskLevel") == "R3", col("LastQrtSD") / zzzs_dict[14110][0])
                                                        .when(col("InitRiskLevel") == "R4", col("LastQrtSD") / zzzs_dict[14110][0])
                                                        .otherwise(0)) \
                         .withColumn("SecondQrtSDFactor",  when(col("InitRiskLevel") == "R2", col("SecondQrtSD") / (0.8 * zzzs_dict[2968][1] + 0.2 * zzzs_dict[14110][1]))
                                                          .when(col("InitRiskLevel") == "R3", col("SecondQrtSD") / zzzs_dict[14110][1])
                                                          .when(col("InitRiskLevel") == "R4", col("SecondQrtSD") / zzzs_dict[14110][1])
                                                          .otherwise(0)) \
                         .withColumn("ThirdQrtSDFactor",  when(col("InitRiskLevel") == "R2", col("ThirdQrtSD") / (0.8 * zzzs_dict[2968][2] + 0.2 * zzzs_dict[14110][2]))
                                                         .when(col("InitRiskLevel") == "R3", col("ThirdQrtSD") / zzzs_dict[14110][2])
                                                         .when(col("InitRiskLevel") == "R4", col("ThirdQrtSD") / zzzs_dict[14110][2])
                                                         .otherwise(0)) \
                         .withColumn("ForthQrtSDFactor",  when(col("InitRiskLevel") == "R2", col("ForthQrtSD") / (0.8 * zzzs_dict[2968][3] + 0.2 * zzzs_dict[14110][3]))
                                                         .when(col("InitRiskLevel") == "R3", col("ForthQrtSD") / zzzs_dict[14110][3])
                                                         .when(col("InitRiskLevel") == "R4", col("ForthQrtSD") / zzzs_dict[14110][3])
                                                         .otherwise(0))

    df_master = df_master.withColumn("LastQrtStandard",  when(col("InitRiskLevel") == "R2", 1.4 * (0.8 * zzzs_dict[2968][0] + 0.2 * zzzs_dict[14110][0]))
                                                        .when(col("InitRiskLevel") == "R3", 1.4 * zzzs_dict[14110][0])
                                                        .when(col("InitRiskLevel") == "R4", 3 * zzzs_dict[14110][0])
                                                        .otherwise(0)) \
                         .withColumn("SecondQrtStandard",  when(col("InitRiskLevel") == "R2", 1.4 * (0.8 * zzzs_dict[2968][1] + 0.2 * zzzs_dict[14110][1]))
                                                          .when(col("InitRiskLevel") == "R3", 1.4 * zzzs_dict[14110][1])
                                                          .when(col("InitRiskLevel") == "R4", 3 * zzzs_dict[14110][1])
                                                          .otherwise(0)) \
                         .withColumn("ThirdQrtStandard",  when(col("InitRiskLevel") == "R2", 1.4 * (0.8 * zzzs_dict[2968][2] + 0.2 * zzzs_dict[14110][2]))
                                                         .when(col("InitRiskLevel") == "R3", 1.4 * zzzs_dict[14110][2])
                                                         .when(col("InitRiskLevel") == "R4", 3 * zzzs_dict[14110][2])
                                                         .otherwise(0)) \
                         .withColumn("ForthQrtStandard",  when(col("InitRiskLevel") == "R2", 1.4 * (0.8 * zzzs_dict[2968][3] + 0.2 * zzzs_dict[14110][3]))
                                                         .when(col("InitRiskLevel") == "R3", 1.4 * zzzs_dict[14110][3])
                                                         .when(col("InitRiskLevel") == "R4", 3 * zzzs_dict[14110][3])
                                                         .otherwise(0))
    
    df_master = df_master.withColumn("SDFlag", sd_flag("LastQrtSD", "SecondQrtSD", "ThirdQrtSD", "ForthQrtSD", "LastQrtStandard", "SecondQrtStandard", "ThirdQrtStandard", "ForthQrtStandard"))
    df_master = df_master.withColumn("SDNote", final_risk("InitRiskLevel", "EstablishmentLength", "NetAssetFlag", "FundSizeStatus", "SDStatus", "SDFlag"))

    df_master = df_master.withColumn("SDStatus", when(col("SDNote").contains("-"), split(col("SDNote"), "-").getItem(3))
                                                .otherwise(col("SDStatus")))\
                         .withColumn("FundSizeStatus", when(col("SDNote").contains("-"), split(col("SDNote"), "-").getItem(2))
                                                      .otherwise(col("FundSizeStatus")))\
                         .withColumn("ChangeReason", when(col("SDNote").contains("-"), split(col("SDNote"), "-").getItem(1))
                                                    .otherwise(col("ChangeReason")))\
                         .withColumn("FinalRiskLevel", when(col("SDNote").contains("-"), split(col("SDNote"), "-").getItem(0))
                                                      .otherwise(col("SDNote")))\
                         .withColumn("ChangeDirection", when(col("ChangeReason").contains("规模上升"), lit(-1))
                                                       .when(col("ChangeReason").contains("波动率下降"), lit(-1))
                                                       .when(col("ChangeReason").contains("规模下降"), lit(1))
                                                       .when(col("ChangeReason").contains("波动率上升"), lit(1))
                                                       .otherwise(lit(0)))

    df_master = df_master.drop("RiskLevel")\
                         .withColumnRenamed("FundTypeName1", "FirstCategoryName")\
                         .withColumnRenamed("FundTypeName2", "SecondCategoryName")\
                         .withColumnRenamed("FundTypeName3", "ThirdCategoryName")\
                         .withColumnRenamed("FundTypeCode1", "FirstCategory")\
                         .withColumnRenamed("FundTypeCode2", "SecondCategory")\
                         .withColumnRenamed("FundTypeCode3", "ThirdCategory")\
                         .withColumnRenamed("InitRiskLevel", "BasicRiskLevel")\
                         .withColumnRenamed("FinalRiskLevel", "RiskLevel")\
                         .withColumnRenamed("SecurityCode", "SecuCode")\
                         .withColumnRenamed("EstablishmentDate", "EstablishDate")

    # 非主代码跟随主代码确定事后风险等级
    df_master = df_master.withColumn("IsMain", when(col("MainCode") == col("SecuCode"), 1)
                                              .otherwise(2))
    windowSpec = Window.partitionBy("MainCode").orderBy(desc("IsMain"))
    df_master = df_master.withColumn("RiskLevel", first("RiskLevel").over(windowSpec))\
                        .withColumn("ChangeDirection", first("ChangeDirection").over(windowSpec))\
                        .withColumn("ChangeReason", first("ChangeReason").over(windowSpec))\
                        .withColumn("SDStatus", first("SDStatus").over(windowSpec))\
                        .withColumn("FundSizeStatus", first("FundSizeStatus").over(windowSpec))\
                        .withColumn("OfficialRiskLevel", first("OfficialRiskLevel").over(windowSpec))

    final_columns = ["InnerCode", "MainCode", "SecuCode", "ApplyingCodeBack", "SecuAbbr", \
                     "EstablishDate", "FirstCategory", "FirstCategoryName", "SecondCategory", "SecondCategoryName", \
                     "ThirdCategory", "ThirdCategoryName", "BasicRiskLevel", "FundSizeStatus", "SDStatus", \
                     "LastQrtNV", "SecondQrtNV", "ThirdQrtNV", "ForthQrtNV", "LastQrtSD", "SecondQrtSD", \
                     "ThirdQrtSD", "ForthQrtSD", "LastQrtSDFactor", "SecondQrtSDFactor", "ThirdQrtSDFactor", \
                     "ForthQrtSDFactor", "RiskLevel", "ChangeDirection", "ChangeReason", "OfficialRiskLevel"]

    df_master = df_master.select(*final_columns)

    df_master = df_master.withColumn("EndDate", lit(date_threshod).cast("date"))\
                         .withColumn("RLDivisionMode", lit(2))\
                         .withColumn("HigherRiskLevel", when(col("OfficialRiskLevel").isNull(), col("RiskLevel"))
                                                       .when(col("RiskLevel") >= col("OfficialRiskLevel"), col("RiskLevel"))
                                                       .otherwise(col("OfficialRiskLevel")))\
                         .withColumn("UpdateTime", current_date())

    df_master = df_master.withColumn("RiskLevelName", level_mapping("RiskLevel"))\
                         .withColumn("OfficialRiskLevelName", level_mapping("OfficialRiskLevel"))\
                         .withColumn("HigherRiskLevelName", level_mapping("HigherRiskLevel"))\
                         .withColumn("Remark", lit(None).cast("string"))\
                         .withColumn("JSID", lit(None).cast("long"))

    df_master = df_master.na.fill(value=0, subset=["ForthQrtNV", "LastQrtNV", "SecondQrtNV", "ThirdQrtNV", \
                                                   "LastQrtSD", "SecondQrtSD", "ThirdQrtSD", "ForthQrtSD", \
                                                   "LastQrtSDFactor", "SecondQrtSDFactor", "ThirdQrtSDFactor", "ForthQrtSDFactor", \
                                                   "FundSizeStatus", "SDStatus"])

    return df_master