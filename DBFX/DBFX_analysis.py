#! /usr/bin/env python 
# encoding: utf-8
'''
@author: zxl
@file: DBFX_analysis.py
@time: 2020/10/28 11:10
'''
"""
杜邦分析数据表格化
"""

import os
import time
import requests
import pandas as pd
from json import loads
from bs4 import BeautifulSoup as BS

class DBFX_Analysis:
    def __init__(self,stocks_id):
        if stocks_id[:2] == '60':
            self.stocks_id = 'SH{}'.format(stocks_id)
        else:
            self.stocks_id = 'SZ{}'.format(stocks_id)
        self.path = os.getcwd().replace('\\','/') + '/files'

    def url_get(self,url_num):
        if url_num == 1:
            url_1 = r'http://f10.eastmoney.com/NewFinanceAnalysis/DubangAnalysisAjax?code={}'.format(self.stocks_id)
            return url_1
        else:
            url_2 = r'http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code={}'.format(self.stocks_id)
            return url_2

    def get_html(self,url_num):
        req = requests.get(self.url_get(url_num))
        if req.status_code == 200:
            print('[+] 网站链接成功。')
        else:
            print('[-] 网站链接失败。')
            exit()
        soup = BS(req.content,'lxml')
        soup_new = loads(soup.get_text())
        return soup_new

    def dataframe(self):
        df = pd.DataFrame(index=range(1,52))
        df.loc[1,0] = '加权净资产收益率'
        df.loc[[2,45,46],1] = ['总资产净利率','归属母公司股东的净利润占比','权益乘数']
        df.loc[[3,21,47,50,51],2] = ['营业净利润率','总资产周转率','资产负债率','资产总额','所有者权益总额']
        df.loc[[4,20,22,23,48,49],3] = ['净利润','营业总收入','营业总收入','资产总额','负债总额','资产总额']
        df.loc[[5,10,24,32],4] = ['收入总额','成本总额','流动资产','非流动资产']
        df.loc[[6,7,8,9,11,12,13,14,15,16,25,26,27,28,29,30,31,33,34,35,36,37,38,39,40,41,42,43,44],5] = ['营业总收入','公允价值变动收益',
                    '营业外收入','投资收益','营业成本','营业税金及附加','所得税费用','资产减值损失','营业外支出','期间费用','货币资金',
                    '交易性金融资产','应收账款','预付账款','其他应收款','存货','其他流动资产','可供出售金融资产','持有至到期投资',
                    '长期股权投资','投资性房地产','固定资产','在建工程','无形资产','开发支出','商誉','长期待摊费用','递延所得税资产',
                    '其他非流动资产']
        df.loc[[17,18,19],6] = ['财务费用','销售费用','管理费用']
        return df

    def to_dataFrame(self,soup,choice):
        df_xx = self.dataframe()
        soup_xx = soup[choice]
        for num in soup_xx:
            today = num['date']
            df_xx.loc[1, today] = num['jzcsyl']
            df_xx.loc[2, today] = num['zzcjll']
            df_xx.loc[45, today] = num['gsmgsgddjlr']
            df_xx.loc[46, today] = num['qycs']
            df_xx.loc[3, today] = num['yyjlrl']
            df_xx.loc[21, today] = num['zzczzl']
            df_xx.loc[47, today] = num['zcfzl']
            df_xx.loc[50, today] = num['zcze']
            if '亿' in num['zcze']:
                df_xx.loc[51, today] = '%.3f亿'%(float(num['zcze'].replace('亿','')) / float(num['qycs']))
            elif '万' in num['zcze']:
                df_xx.loc[51, today] = '%.3f亿'%((float(num['zcze'].replace('万', ''))/10000) / float(num['qycs']))
            df_xx.loc[4, today] = num['jlr']
            df_xx.loc[20, today] = num['yysr']
            df_xx.loc[22, today] = num['yysr']
            df_xx.loc[23, today] = num['zcze']
            df_xx.loc[48, today] = num['fzze']
            df_xx.loc[49, today] = num['zcze']
            df_xx.loc[5, today] = num['srze']
            df_xx.loc[10, today] = num['cbze']
            df_xx.loc[24, today] = num['ldzc']
            df_xx.loc[32, today] = num['fldzc']
            df_xx.loc[6, today] = num['yysr']
            df_xx.loc[7, today] = num['gyjzbdsy']
            df_xx.loc[8, today] = num['yywsr']
            df_xx.loc[9, today] = num['tzsy']
            df_xx.loc[11, today] = num['yycb']
            df_xx.loc[12, today] = num['yysjjfj']
            df_xx.loc[13, today] = num['sdsfy']
            df_xx.loc[14, today] = num['zcjzss']
            df_xx.loc[15, today] = num['yywzc']
            df_xx.loc[16, today] = num['qjfy']
            df_xx.loc[16, today] = num['qjfy']
            df_xx.loc[25, today] = num['hbzj']
            df_xx.loc[26, today] = num['jyxjrzc']
            df_xx.loc[27, today] = num['yszk']
            df_xx.loc[28, today] = num['yfzk']
            df_xx.loc[29, today] = num['qtysk']
            df_xx.loc[30, today] = num['ch']
            df_xx.loc[31, today] = num['qtldzc']
            df_xx.loc[33, today] = num['kgcsjrzc']
            df_xx.loc[34, today] = num['cyzdqtz']
            df_xx.loc[35, today] = num['cqgqtz']
            df_xx.loc[36, today] = num['tzxfdc']
            df_xx.loc[37, today] = num['gdzc']
            df_xx.loc[38, today] = num['zjgc']
            df_xx.loc[39, today] = num['wxzc']
            df_xx.loc[40, today] = num['kfzc']
            df_xx.loc[41, today] = num['sy']
            df_xx.loc[42, today] = num['cqdtfy']
            df_xx.loc[43, today] = num['dysdszc']
            df_xx.loc[44, today] = num['qtfldzc']
            df_xx.loc[17, today] = num['cwfy']
            df_xx.loc[18, today] = num['xsfy']
            df_xx.loc[19, today] = num['glfy']
        return df_xx

    def ldfzzb(self):    # 流动负债占比
        soup_xx = self.get_html(url_num = 2)
        df_xx = pd.DataFrame(index = range(1,40))
        index_values = [2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,21,22,23,24,25,26,28,29,30,32,33,34,36,37,38,39]
        df_xx.loc[[1,9,20,27,31,35],0] = ['每股指标','成长能力指标','盈利能力指标','盈利质量指标','运营能力指标','财务风险指标']
        df_xx.loc[index_values,1] = ['基本每股收益(元)',
                '扣非每股收益(元)','稀释每股收益(元)','每股净资产(元)','每股公积金(元)','每股未分配利润(元)','每股经营现金流(元)',
                '营业总收入(元)','毛利润(元)','归属净利润(元)','扣非净利润(元)','营业总收入同比增长(%)','归属净利润同比增长(%)',
                '扣非净利润同比增长(%)','营业总收入滚动环比增长(%)','归属净利润滚动环比增长(%)','扣非净利润滚动环比增长(%)',
                '加权净资产收益率(%)','摊薄净资产收益率(%)','摊薄总资产收益率(%)','毛利率(%)','净利率(%)','实际税率(%)',
                '预收款/营业收入','销售现金流/营业收入','经营现金流/营业收入','总资产周转率(次)','应收账款周转天数(天)',
                '存货周转天数(天)','资产负债率(%)','流动负债/总负债(%)','流动比率','速动比率']
        for num in soup_xx:
            today = num['date']
            df_xx.loc[index_values,today] = list(num.values())[1:]

        return df_xx


    def dbfx(self):
        soup_new = self.get_html(url_num = 1)
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        df_0 = self.to_dataFrame(soup_new,'bgq')
        df_1 = self.to_dataFrame(soup_new,'nd')
        df_2 = self.ldfzzb()
        writer = pd.ExcelWriter(self.path+'/{0}.xls'.format(self.stocks_id))
        df_0.to_excel(writer,'季报',index=False)
        df_1.to_excel(writer,'年报',index=False)
        df_2.to_excel(writer, '主要指标', index=False)
        writer.save()


if __name__ == '__main__':
    stocks = '000936'                      # 'SZ000681','SH600132'
    if type(stocks) == str:
        print('[+] 正在汇总{0}信息。'.format(stocks))
        xx = DBFX_Analysis(stocks)
        xx.dbfx()
    elif type(stocks) == list:
        for stock in stocks:
            print('[+] 正在汇总{0}信息。'.format(stock))
            xx = DBFX_Analysis(stock)
            xx.dbfx()
            time.sleep(1)
