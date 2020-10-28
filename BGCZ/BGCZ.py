#! /usr/bin/env python 
# encoding: utf-8
'''
@author: zxl
@file: BGCZ.py
@time: 2020/10/9 10:41
'''
### 并购重组策略汇总

import os
import time
import pickle
import shutil
import requests
import pandas as pd
from json import loads
from bs4 import BeautifulSoup as BS


class BGCZ:
    def __init__(self):
        self.path = os.getcwd().replace('\\','/')
        self.total_dict = {}
        self.columns = ['股票代码', '股票简称', '交易标的', '买方', '卖方', '交易金额(万元)', '币种', '股权转让比例(%)','并购方式','最新进展','披露日期','最新公告日','所在页码']
        self.header = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                    'Connection': 'keep-alive',
                    'Referer': 'http://www.baidu.com/'
                }

    def url_get(self,num):
        url_ = r'http://datacenter.eastmoney.com/api/data/get?type=RPTA_WEB_BGCZMX&sty=ALL&source=WEB&p={0}&ps=50&st=scggrq&sr=-1&var=ZAIJzPYz&rt=53456597'.format(num)
        return url_

    def get_http(self,num):
        req = requests.get(self.url_get(num),headers = self.header)
        if req.status_code != 200:
            print('[+] 网址请求有问题，请检查！！！')
            exit()
        soup = BS(req.content,'lxml')

        # # 罗列各个专栏的网址
        # items_dict = {x.text:x['href'] for x in soup.find_all(class_="nav")[0].find_all('a')}
        # print(soup)

        # 寻找并购重组专栏
        item_BG = soup.get_text()[4:].replace(';','')
        item_BG = loads(item_BG.split('=')[1])
        item_BG_pages = item_BG['result']['pages']
        item_BG_datas = item_BG['result']['data']
        return item_BG_pages,item_BG_datas

    def transform(self):
        tot_pages, BG_datas = self.get_http(1)

        for num in range(1,tot_pages+1):
            print('[+] 现在在遍历第{}页。'.format(num))
            tot_pages, BG_datas = self.get_http(num)
            for data_ in BG_datas:
                stock_coid = data_['SCODE']
                if stock_coid not in list(self.total_dict.keys()):
                    self.total_dict[stock_coid] = pd.DataFrame(columns = self.columns)
                len_num = len(self.total_dict[stock_coid])
                self.total_dict[stock_coid].loc[len_num, '股票代码'] = data_['SCODE']
                self.total_dict[stock_coid].loc[len_num, '股票简称'] = data_['SNAME']
                self.total_dict[stock_coid].loc[len_num, '交易标的'] = data_['H_COMNAME']
                self.total_dict[stock_coid].loc[len_num, '买方'] = data_['S_COMNAME']
                self.total_dict[stock_coid].loc[len_num, '卖方'] = data_['G_GOMNAME']
                self.total_dict[stock_coid].loc[len_num, '交易金额(万元)'] = data_['JYJE']
                self.total_dict[stock_coid].loc[len_num, '币种'] = data_['BZNAME']
                self.total_dict[stock_coid].loc[len_num, '股权转让比例(%)'] = data_['ZRBL']
                self.total_dict[stock_coid].loc[len_num, '并购方式'] = data_['ZRFS']
                self.total_dict[stock_coid].loc[len_num, '最新进展'] = data_['JD']
                self.total_dict[stock_coid].loc[len_num, '披露日期'] = str(data_['SCGGRQ']).split(' ')[0]
                self.total_dict[stock_coid].loc[len_num, '最新公告日'] = str(data_['ANNOUNDATE']).split(' ')[0]
                self.total_dict[stock_coid].loc[len_num, '所在页码'] = num
            time.sleep(2)
        self.save_pkl()

    def save_pkl(self):
        with open(self.path + '/total_dict.pkl','wb') as fr:
            pickle.dump(self.total_dict,fr)
            fr.close()

    def load_pkl(self):
        with open(self.path + '/total_dict.pkl','rb') as fr:
            df = pickle.load(fr)
            fr.close()

        path_file = self.path + '/csv'
        if os.path.exists(path_file):
            shutil.rmtree(path_file)
        if not os.path.exists(path_file):
            os.mkdir(path_file)
        for stock in df.keys():
            print(stock)
            stock_name = df[stock].loc[0,'股票简称'].replace('*','')
            df[stock].drop_duplicates(subset=['交易标的','买方','卖方','交易金额(万元)','币种','股权转让比例(%)','并购方式',
                                              '最新进展','披露日期','最新公告日'],keep='last',inplace=True)
            try:
                df[stock].to_excel(path_file + '/{}_{}.xls'.format(stock, stock_name), encoding='gbk')
            except:
                if '{}_{}.xls'.format(stock,stock_name) not in list(os.listdir(path_file)):
                    df[stock].to_csv(path_file+'/{}_{}.csv'.format(stock,stock_name),encoding='gbk')
                else:
                    df = pd.read_excel(path_file + '/{}_{}.xls'.format(stock, stock_name), encoding='gbk')
                    df.append(df[stock])
                    df.to_csv(path_file + '/{}_{}.csv'.format(stock, stock_name), encoding='gbk')

    def concat_data(self):
        with open(self.path + '/total_dict.pkl','rb') as fr:
            self.total_dict = pickle.load(fr)
            fr.close()
        for num in range(1,10):
            print('[+] 现在在遍历第{}页。'.format(num))
            tot_pages, BG_datas = self.get_http(num)
            for data_ in BG_datas:
                stock_coid = data_['SCODE']
                if stock_coid not in list(self.total_dict.keys()):
                    self.total_dict[stock_coid] = pd.DataFrame(columns = self.columns)
                len_num = len(self.total_dict[stock_coid])
                if len(self.total_dict[stock_coid]) >= 1:
                    if ((self.total_dict[stock_coid]['交易标的'].iloc[0] == data_['H_COMNAME']) and (self.total_dict[stock_coid]['买方'].iloc[0] == data_['S_COMNAME'])
                        and (self.total_dict[stock_coid]['卖方'].iloc[0] == data_['G_GOMNAME']) and (self.total_dict[stock_coid]['交易金额(万元)'].iloc[0] == data_['JYJE'])
                        and (self.total_dict[stock_coid]['币种'].iloc[0] == data_['BZNAME']) and (self.total_dict[stock_coid]['股权转让比例(%)'].iloc[0] == data_['ZRBL'])
                        and (self.total_dict[stock_coid]['并购方式'].iloc[0] == data_['ZRFS']) and (self.total_dict[stock_coid]['最新进展'].iloc[0] == data_['JD'])
                        and (self.total_dict[stock_coid]['披露日期'].iloc[0] == str(data_['SCGGRQ']).split(' ')[0]) and (self.total_dict[stock_coid]['最新公告日'].iloc[0] == str(data_['ANNOUNDATE']).split(' ')[0])):
                        continue
                self.total_dict[stock_coid].loc[len_num, '股票代码'] = data_['SCODE']
                self.total_dict[stock_coid].loc[len_num, '股票简称'] = data_['SNAME']
                self.total_dict[stock_coid].loc[len_num, '交易标的'] = data_['H_COMNAME']
                self.total_dict[stock_coid].loc[len_num, '买方'] = data_['S_COMNAME']
                self.total_dict[stock_coid].loc[len_num, '卖方'] = data_['G_GOMNAME']
                self.total_dict[stock_coid].loc[len_num, '交易金额(万元)'] = data_['JYJE']
                self.total_dict[stock_coid].loc[len_num, '币种'] = data_['BZNAME']
                self.total_dict[stock_coid].loc[len_num, '股权转让比例(%)'] = data_['ZRBL']
                self.total_dict[stock_coid].loc[len_num, '并购方式'] = data_['ZRFS']
                self.total_dict[stock_coid].loc[len_num, '最新进展'] = data_['JD']
                self.total_dict[stock_coid].loc[len_num, '披露日期'] = str(data_['SCGGRQ']).split(' ')[0]
                self.total_dict[stock_coid].loc[len_num, '最新公告日'] = str(data_['ANNOUNDATE']).split(' ')[0]
                self.total_dict[stock_coid].loc[len_num, '所在页码'] = num
            time.sleep(2)
        self.save_pkl()




if __name__ == '__main__':
    xx = BGCZ()
    xx.transform()
    xx.load_pkl()
