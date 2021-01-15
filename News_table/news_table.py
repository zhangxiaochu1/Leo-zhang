#! /usr/bin/env python 
# encoding: utf-8
'''
@author: zxl
@file: news_table.py
@time: 2021/1/15 10:28
'''

import os
import time
import requests
import pandas as pd
from json import loads
from bs4 import BeautifulSoup as BS


class NEWS:
    def __init__(self,choice):
        self.choice = choice
        if self.choice == '7_24':
            self.path = os.getcwd().replace('\\','/') + '/7_24_NEWS.csv'
            self.url = r'https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_{0}_.html?r=' \
                       r'0.048019601140350865&_=1610677534537'
        else:
            self.path = os.getcwd().replace('\\','/') + '/key_point_NEWS.csv'
            self.url = r'https://newsapi.eastmoney.com/kuaixun/v1/getlist_101_ajaxResult_50_{0}_.html?r=' \
                     r'0.3700752195043526&_=1610689572387'
        self.cookie = r'em_hq_fls=js; ct=Sm7Lv4k861KzoMzcq_z-t4f78JZUezVpbh0w9MyuBj0u6sJDEQYbNSRVE-gq5cPBXaZ' \
                      r'fjjhMyjcWok8Lxca_2boupMxP4BexEGXsMNvSB-sueGKJpL3FOlfVplvseY2Y3905Js1mrOCjFXfNXPm8Lzf' \
                      r'e93ab5FdOwkQ7pGPu0-Y; ut=FobyicMgeV4ukx1pWSXZAgc6gjh2MS1d3PN-BMW3QN9PJpxon-RU9V3cxby' \
                      r'iSJsCRkhsJsAI2YfeYEJZc0vcDDHg8GBb6Q2geL1hFslyIsUgNrKVwedZIVigQdqGXgEBMd4UvNP0XI5m3tt' \
                      r'qehDovceVtqpb75ngzdVWP5YejVZ40Y74vWbYWuQ4aBU5O32gCBef8PaHKrwag_8fCTfopJXKUcEoePW6ye4' \
                      r'Dhr_FJZjtNZlcq0i1lwsWlZS4TDkkza2MjSiPbofuC-9y0kygZmY4qSzNFVlC; pi=9748075015607956%3' \
                      r'bf9748075015607956%3b%e5%bc%a0%e5%b0%8f%e9%a5%ad123%3bPJvpFHSUT%2b704ntixgYFr5jV7a8o' \
                      r'JCj64DOcmJGk7I9NfyQR3JOAakqFLZNezbuI6vGZPb%2blMnYxAiRE3kCjJHm7iVadWl5BIhIamLa0OFhpKO' \
                      r'c1EyRcAeQBOWuJJbts53aIF3iMdGx%2fr%2fuhYdMbjE4FWe8bSkHg1ClweyCP4EIzqRGGQwzlA4gIDp3SoJ' \
                      r'p5oWGG4XkY%3bmTBD4s0RtlgTXhBk%2f232XS316Cpm%2fZkMguPmecj1lUm%2fkebLYR90E4l%2b%2bn6%2' \
                      r'btNugYEuIABsTFvYRZzbwXc17vjUHN2x0NJO2tmJ%2bnCLTt2%2fqnBaho1f3PJPzOBdn2%2bK3Tf7%2fxt%' \
                      r'2bLSvrmL5LZ5zwPYPUGQbulJA%3d%3d; uidal=9748075015607956%e5%bc%a0%e5%b0%8f%e9%a5%ad12' \
                      r'3; sid=110592060; vtpst=%7c; _qddaz=QD.jl642l.bw0rk.kclsngek; pgv_pvi=475404288; em-' \
                      r'quote-version=topspeed; cowminicookie=true; intellpositionL=618px; st_si=40595824636' \
                      r'940; emshistory=%5B%22%E5%AE%8B%E5%9F%8E%E6%BC%94%E8%89%BA%22%2C%22scyy%22%2C%220006' \
                      r'81%22%2C%220000681%22%2C%22%E8%A7%86%E8%A7%89%E4%B8%AD%E5%9B%BD%22%2C%22sjzg%22%2C%2' \
                      r'2%E7%94%9F%E7%89%A9%E8%82%A1%E4%BB%BD%22%2C%22002138%22%2C%22gyzb%22%2C%22%E4%B8%AD%' \
                      r'E4%BF%A1%E5%BB%BA%E6%8A%95%22%2C%22600585%22%2C%22600745%22%2C%22%E9%97%BB%E6%B3%B0%' \
                      r'E7%A7%91%E6%8A%80%22%2C%22%E4%B8%BB%E5%8A%A8%E4%B9%B0%E5%85%A5%E9%87%8F%22%2C%22%E5%' \
                      r'9F%BA%E8%9B%8B%E7%94%9F%E7%89%A9%22%2C%22300144%22%2C%22%E5%A4%96%E6%B1%87%E5%92%8C%' \
                      r'E9%BB%84%E9%87%91%E5%82%A8%E5%A4%87%22%2C%22%E5%AE%81K0KG603387%22%2C%22%E9%95%BF%E7' \
                      r'%9B%88%E7%B2%BE%E5%AF%86%22%2C%22%E5%8C%97%E5%90%91%E8%B5%84%E9%87%91%22%5D; HAList=' \
                      r'a-sz-300144-%u5B8B%u57CE%u6F14%u827A%2Ca-sz-000010-%u7F8E%u4E3D%u751F%u6001%2Ca-sh-6' \
                      r'03259-%u836F%u660E%u5EB7%u5FB7%2Ca-sz-000681-%u89C6%u89C9%u4E2D%u56FD%2Ca-sz-300482-' \
                      r'%u4E07%u5B5A%u751F%u7269%2Ca-sz-000403-%u53CC%u6797%u751F%u7269%2Ca-sz-002138-%u987A' \
                      r'%u7EDC%u7535%u5B50%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sh-600201-%u751F%u7269' \
                      r'%u80A1%u4EFD%2Ca-sh-601066-%u4E2D%u4FE1%u5EFA%u6295%2Ca-sh-600585-%u6D77%u87BA%u6C34' \
                      r'%u6CE5; st_asi=delete; intellpositionT=1155px; qgqp_b_id=dd320c728b86e7b297426bab328' \
                      r'c6c99; waptgshowtime=2021115; st_pvi=28822081217583; st_sp=2020-03-13%2012%3A33%3A49' \
                      r'; st_inirUrl=https%3A%2F%2Fwww.so.com%2Flink; st_sn=1299; st_psi=20210115102501135-1' \
                      r'13103303721-9096147462'
        self.header = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                    'Connection': 'keep-alive',
                    'Referer': 'http://www.baidu.com/'
                }

    def get_html(self,num):
        req = requests.post(url=self.url.format(num),data=self.cookie,headers = self.header)
        if req.status_code != 200:
            print('[-] 网址未连接，请检查数据！')
            quit()
        soup = BS(req.content,'lxml')
        soup_text = soup.get_text()
        soup_new = loads(soup_text.split('=')[1])
        return soup_new['LivesList']

    def main(self):
        if not os.path.exists(self.path):
            df_frame = pd.DataFrame(columns = ['showtime','title','digest','url_unique'])
        else:
            df_frame = pd.read_csv(self.path,encoding='gbk')
        for num in range(1,21):
            time.sleep(3)
            Lives_List = self.get_html(num)
            for id in list(Lives_List):
                print('[+] ',id['title'])
                if id['url_unique'] not in list(df_frame['url_unique']):
                    len_num = len(df_frame)
                    showtime = id['showtime']
                    title = id['title']
                    digest = id['digest']
                    url_unique = id['url_unique']
                    df_frame.loc[len_num] = [showtime,title,digest,url_unique]
                else:
                    break
            if id['url_unique'] in list(df_frame['url_unique']):
                break
        df_frame = df_frame.sort_values(by=['showtime'],ascending=False)
        df_frame.to_csv(self.path,encoding='gbk',index=False)



if __name__ == '__main__':
    NEW_7_24 = NEWS('7_24')
    NEW_key_point = NEWS('key_point')
    while True:
        print('**************  7 * 24  ******************')
        NEW_7_24.main()
        print('++++++++++++  key_point ++++++++++++++++++')
        NEW_key_point.main()
        time.sleep(600)
