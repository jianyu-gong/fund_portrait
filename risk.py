from pyspark.sql.functions import *

@udf
def cal_risk(ChiName, FundTypeCode1, FundTypeCode2, RiskLevel):
    """
    重新定义一般基金
    检查ChiName, FundTypeCode1, FundTypeCode2字段, 根据搜索关键字
    覆盖原本的风险等级
    """
    # QDII基金
    if FundTypeCode1 == "14" and FundTypeCode2 == "1450":
        # 商品QDII基金（除黄金QDII）
        if "原油" in ChiName or "商品" in ChiName or "通胀" in ChiName:
            return "R5"
        # 黄金QDII基金
        elif "黄金" in ChiName:
            return "R3"
        else:
            return RiskLevel
        
    elif FundTypeCode1 == "17":
        if "期货" in ChiName:
            return "R5"
        elif "黄金" in ChiName or "上海金" in ChiName:
            return "R3"
        else:
            return RiskLevel
        
    elif FundTypeCode1 == "15":
        return "R3"
    
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