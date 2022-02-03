from risk import pre_process_data, pre_fund_risk_calc, previous_quarter, post_fund_risk_calc, volatilityByQuarter
from datetime import datetime

class Fund_Label(object):

    def fund_risk_estimate(self, df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, risk_mapping, df_mainfinancialindex, df_last_qrt, df_unitnvrestored, df_zzqz, df_zzzcf):
        # 根据当前日期取上一季度的最后一天
        date_threshod, date_threshod_2, date_threshod_3, date_threshod_4 = previous_quarter(datetime.now())
        tmp_date = datetime.strptime(date_threshod, "%Y-%m-%d")
        tmp_date_2 = datetime.strptime(date_threshod_2, "%Y-%m-%d")
        tmp_date_3 = datetime.strptime(date_threshod_3, "%Y-%m-%d")
        tmp_date_4 = datetime.strptime(date_threshod_4, "%Y-%m-%d")

        date_threshod_5 = datetime(tmp_date.year-1, tmp_date.month, tmp_date.day).strftime('%Y-%m-%d')
        date_threshod_6 = datetime(tmp_date.year-3, tmp_date.month, tmp_date.day).strftime('%Y-%m-%d')
        date_threshod_7 = datetime(tmp_date_2.year-3, tmp_date_2.month, tmp_date_2.day).strftime('%Y-%m-%d')
        date_threshod_8 = datetime(tmp_date_3.year-3, tmp_date_3.month, tmp_date_3.day).strftime('%Y-%m-%d')
        date_threshod_9 = datetime(tmp_date_4.year-3, tmp_date_4.month, tmp_date_4.day).strftime('%Y-%m-%d')

        # 预处理
        print("正在预处理数据")
        master_df = pre_process_data(df_fundarchives, df_secumain, df_fundtype, df_fundrisklevel, df_fundtypechangenew, date_threshod)
        # 事前风险计算
        print("正在计算事前风险")
        master_df = pre_fund_risk_calc(master_df, risk_mapping, date_threshod, date_threshod_2)
        # 事后风险计算
        print("正在计算事后风险")
        # 计算基金波动率
        df_unitnvrestored_master = volatilityByQuarter(df_unitnvrestored, date_threshod, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_6, date_threshod_7, date_threshod_8, date_threshod_9, "unitnvrestored")
        # 计算中证指数波动率
        df_zz = df_zzqz.unionByName(df_zzzcf)
        df_zz = volatilityByQuarter(df_zz, date_threshod, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_6, date_threshod_7, date_threshod_8, date_threshod_9, "zz")
        print("正在计算中证指数波动率")
        zzzs = df_zz.collect()

        zzzs_dict = {}

        for item in zzzs:
            zzzs_dict[item[0]] = [item[1], item[2], item[3], item[4]]
            
        print("正在计算最终风险")
        master_df = post_fund_risk_calc(master_df, date_threshod_2, date_threshod_3, date_threshod_4, date_threshod_5, date_threshod, df_mainfinancialindex, df_last_qrt, df_unitnvrestored_master, zzzs_dict)

        print("正在导出数据")
        master_df.repartition(1).write.csv("result_spark",header=True) 
        # master_df.show()