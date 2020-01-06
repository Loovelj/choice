# python 3.7.4
# coding = utf-8
# filename get_stock_codes.py
# author 463714869@qq.com/www.cdzcit.com,
#        create by VIM at 2019/12/30

from EMQuantAPI import EmQuantAPI
from utils import showEMQuantErrMessage
from datasets import EMQuantCodesList
import time
import pymysql

db_host = '47.108.57.15'
db_port = 3306
db_user = 'root'
db_password = '1AjLAEvQvuR4TwaP'
db_name = 'zccm_lhua'

if __name__ == '__main__':

    # 登录Choice
    loginResult = EmQuantAPI.c.start()
    if loginResult.ErrorCode != 0:
        showEMQuantErrMessage(loginResult.ErrorCode)
        exit()

    # 登录DB
    conn = pymysql.connect(host=db_host,
                           user=db_user,
                           passwd=db_password,
                           port=db_port,
                           db=db_name,
                           charset='utf8')
    cursor = conn.cursor()

    # 拉取深沪股票信息 - 市场类
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    for item in EMQuantCodesList:
        for key in item.data:
            result = EmQuantAPI.c.sector(key, today)
            if result.ErrorCode != 0:
                showEMQuantErrMessage(result.ErrorCode)
                continue
            for i in range(len(result.Data) // 2):
                sql = 'INSERT INTO t_stock_market VALUES ("%s", "%s", "%s", "%s")' % \
                      (result.Data[i * 2], result.Data[i * 2 + 1],
                       item.name.replace('-', '').replace(' ', ''),
                       item.data[key].replace('-', '').replace(' ', ''))
                cursor.execute(sql)
                cursor.execute('commit')
    cursor.close()
    conn.close()

    EmQuantAPI.c.stop()
