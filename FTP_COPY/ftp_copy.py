#! /usr/bin/env python 
# encoding: utf-8
'''
@author: zxl
@file: ftp_copy.py
@time: 2020/11/26 9:57
'''


import os
import zipfile
from ftplib import FTP
from datetime import date


class FTP_COPY:
    def __init__(self):
        self.date = str(date.today()).replace('-','')
        # self.date = '20201102'
        self.remotepath = '/mscibarra/NO_DATE/'
        self.localpath = r'\\192.168.3.126\accounts\CNTR_barra'
        self.month_dict = {"01": 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr', '05': 'May', '06': 'Jun', '07': 'Jul',
                           '08': 'Aug', '09': 'Sept', '10': 'Oct', '11': 'Nov', '12': 'Dec'}
        self.date_1 = self.date[:4] + self.month_dict[self.date[4:6]] + self.date[-2:]

        # 初始化
        self.ftpconnect('172.16.0.63', 'admin', 'h4x18d')

    def ftpconnect(self, host, username, password):
        ftp = FTP()
        ftp.set_debuglevel(0)
        ftp.connect(host, 21)
        ftp.login(username, password)
        ftp.encoding = 'UTF-8'
        ftp.set_pasv(0)
        ftp.passiveserver = 0
        ftp.cwd(self.remotepath)
        self.ftp = ftp

    # 从FTP上下载文件
    def downloadfile(self, remotepath, localpath):
        bufsize = 1024
        fp = open(localpath, 'wb')
        self.ftp.retrbinary('RETR ' + remotepath, fp.write, bufsize)
        self.ftp.set_debuglevel(0)
        fp.close()

    # 从本地上传文件到ftp
    def uploadfile(self, remotepath, localpath):
        bufsize = 1024
        fp = open(localpath, 'rb')
        self.ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        self.ftp.set_debuglevel(0)
        fp.close()

    # 解压缩包
    def zip_extract(self, from_path, to_path):
        print(u'----正在解压文件----')
        zip_file = zipfile.ZipFile(from_path)

        for names in zip_file.namelist():
            zip_file.extract(names, to_path)
        zip_file.close()
        print(u'----解压结束----')

    # 压缩文件夹
    def zip_file(self):
        local_path = os.path.join(self.localpath, self.date)
        zip_name = local_path + '.zip'
        z = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
        for dirpath, dirname, filenames in os.walk(local_path):
            fpath = dirpath.replace(local_path, "")
            fpath = fpath and fpath + os.sep or ""
            for filename in filenames:
                z.write(os.path.join(dirpath, filename), fpath + filename)
            print(fpath.replace('\\', '') + ' success to_zip!!!')
        z.close()

    # 循环遍历文件
    def files_download(self):
        files_list = [x for x in self.ftp.nlst() if (self.date in x) or (self.date[-6:] in x) or (self.date_1) in x]
        files_list_zip = [x for x in files_list if '.zip' in x]
        if not os.path.exists(os.path.join(self.localpath, self.date)):
            os.mkdir(os.path.join(self.localpath, self.date))
        for file in files_list:
            file_path = os.path.join(self.remotepath, file)
            to_path = os.path.join(self.localpath, self.date, file)
            self.downloadfile(file_path, to_path)

        for file_1 in files_list_zip:
            file_path_1 = os.path.join(self.localpath, self.date, file_1)
            to_path_1 = os.path.join(self.localpath, self.date, file_1.split('.')[0])
            self.zip_extract(file_path_1, to_path_1)


if __name__ == '__main__':
    xx = FTP_COPY()
    xx.files_download()
