from risk import cal_risk, cal_speical_fund_risk

class Fund_Label(object):

    def fund_risk_estimate(self, master_df, risk_mapping):
        master_df = master_df.join(risk_mapping, ["FundTypeName1", "FundTypeName2", "FundTypeName3"], "left")
        master_df = master_df.withColumn("InitRiskLevel", cal_risk("ChiName", "FundTypeCode1", "FundTypeCode2", "RiskLevel"))
        master_df = master_df.withColumn("InitRiskLevel", cal_speical_fund_risk("ChiName", "SecurityCode", "InitRiskLevel"))
        master_df.toPandas().to_csv("result.csv", header=True)