from risk import pre_process_data, pre_fund_risk_calc, previous_quarter, post_fund_risk_calc
from datetime import datetime

class Fund_Label(object):

    def fund_risk_estimate(self, df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping, df_mainfinancialindex, df_last_qrt):
        # 根据当前日期取上一季度的最后一天
        date_threshod, date_threshod_2, date_threshod_3, date_threshod_4 = previous_quarter(datetime.now())
        tmp_date = datetime.strptime(date_threshod, "%Y-%m-%d")
        date_threshod_5 = datetime(tmp_date.year-1, tmp_date.month, tmp_date.day).strftime('%Y-%m-%d')
        # 预处理
        print("正在预处理数据")
        master_df = pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, date_threshod)
        # 事前风险计算
        print("正在计算事前风险")
        master_df = pre_fund_risk_calc(master_df, risk_mapping, date_threshod, date_threshod_2)
        # 事后风险计算
        print("正在计算事后风险")
        master_df = post_fund_risk_calc(master_df, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_5, date_threshod, df_mainfinancialindex, df_last_qrt)

        print("正在导出数据")
        master_df.repartition(1).write.csv("result_spark",header=True) 
        # master_df.show()