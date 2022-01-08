from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import datetime

@udf
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

@udf
def cal_speical_fund_risk(ChiName, SecurityCode, InitRiskLevel):
    # 新三板基金SecurityCode
    neeq_r3_list = ["009697", "009698", "009693", "009688", "009681", "009682", "009683", "009684", "009695", "009696", "009867", "009868", 
                    "010887", "010888", "012107", "012108", "011886", "011887", "011011", "010646", "010647", "012850", "012851"]
    neeq_r4_list = ["011783", "011530", "910006", "910009", "010442", "011790"]
    
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

@udf
def tranform_text(PreviousFundTypeName3):
    if PreviousFundTypeName3:
        return ("由%s转型而来")%(PreviousFundTypeName3)
    else:
        return 

def previous_quarter(ref):
    if ref.month < 4:
        return datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 9, 30).strftime('%Y-%m-%d')
    elif ref.month < 7:
        return datetime(ref.year, 3, 31).strftime('%Y-%m-%d'), datetime(ref.year-1, 12, 31).strftime('%Y-%m-%d')
    elif ref.month < 10:
        return datetime(ref.year, 6, 30).strftime('%Y-%m-%d'), datetime(ref.year, 3, 31).strftime('%Y-%m-%d')
    return datetime(ref.year, 9, 30).strftime('%Y-%m-%d'), datetime(ref.year, 6, 30).strftime('%Y-%m-%d')


def pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, date_threshod):
    """
    根据query来整合基金的type和名称
    """

    # SecuCategory IN (8,13) and ListedState IN (1,9)
    df_secumain = df_secumain.filter(((col("SecuCategory") == 8) | (col("SecuCategory") == 13)) & ((col("ListedState") == 1) | (col("ListedState") == 9)))

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

    df_master = df_master.withColumn("ChangeReasons", tranform_text("PreviousFundTypeName3"))

    return df_master