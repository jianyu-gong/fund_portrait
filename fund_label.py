from risk import pre_process_data, pre_fund_risk_calc

class Fund_Label(object):

    def fund_risk_estimate(self, df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping):
        # 预处理
        print("正在预处理数据")
        master_df = pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew)
        # 事前风险计算
        print("正在计算事前风险")
        master_df = pre_fund_risk_calc(master_df, risk_mapping)
        print("正在导出数据")
        master_df.coalesce(1).write.csv("result_spark",header=True) 
        # master_df.show()