from risk import cal_risk, cal_speical_fund_risk, pre_process_data

class Fund_Label(object):

    def fund_risk_estimate(self, df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping):

        master_df = pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew)
        master_df = master_df.join(risk_mapping, ["FundTypeName1", "FundTypeName2", "FundTypeName3"], "left")
        master_df = master_df.withColumn("InitRiskLevel", cal_risk("ChiName", "FundTypeCode1", "FundTypeCode2", "RiskLevel"))
        master_df = master_df.withColumn("InitRiskLevel", cal_speical_fund_risk("ChiName", "SecurityCode", "InitRiskLevel"))
        master_df.toPandas().to_csv("result.csv", header=True)