# python 3.7.4
# coding = utf-8
# filename get_stock_codes.py
# author 463714869@qq.com/www.cdzcit.com,
#        create by VIM at 2019/12/30

from EMQuantAPI import EmQuantAPI
from utils import showEMQuantErrMessage
from datasets import EMQuantCodesList_Concept, EMQuantCodesList_Market, EMQuantCodesList_DongCai
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
    for item in EMQuantCodesList_Market:
        for key_1 in item:
            for key in item[key_1]:
                result = EmQuantAPI.c.sector(key, today)
                if result.ErrorCode != 0:
                    showEMQuantErrMessage(result.ErrorCode)
                    continue
                for i in range(len(result.Data) // 2):
                    # 获取市场分类ID
                    res = cursor.execute('SELECT * FROM t_stock_market_classes where stock_market_code="%s"' % key)
                    if res != 1:
                        print('Unkown market code: %s' % key)
                        break
                    cid = cursor.fetchone()[0]
                    # 检查是否已存在该股票数据
                    res = cursor.execute('SELECT market_category_id FROM t_stock where code="%s"' % result.Data[i * 2])
                    if res == 0:
                        sql = 'INSERT INTO t_stock (code, name, market_category_id) VALUES ("%s", "%s", "%s,")' % \
                              (result.Data[i * 2], result.Data[i * 2 + 1], cid)
                    else:
                        midstr = cursor.fetchone()[0] + ('%s,' % cid)
                        sql = 'UPDATE t_stock SET market_category_id = "%s" WHERE code = "%s"' % \
                              (midstr, result.Data[i * 2])
                    cursor.execute(sql)
                    conn.commit()

    # 拉取深沪股票信息 - 东财分类
    for item in EMQuantCodesList_DongCai:
        # 一级分类
        for key_1 in item:
            # 二级分类
            for key_2 in item[key_1]:
                # 三级分类
                for key in item[key_2]:
                    result = EmQuantAPI.c.sector(key, today)
                    if result.ErrorCode != 0:
                        showEMQuantErrMessage(result.ErrorCode)
                        continue
                    for i in range(len(result.Data) // 2):
                        # 检查是否已存在该股票数据
                        res = cursor.execute(
                            'SELECT market_category_id FROM t_stock where code="%s"' % result.Data[i * 2])
                        if res == 0:
                            sql = 'INSERT INTO t_stock ' \
                                  '(code, dc_category,' \
                                  'dc_subcategory_1,' \
                                  'dc_subcategory_1_code,' \
                                  'dc_subcategory_2,' \
                                  'dc_subcategory_2_code) VALUES ("%s", "%s", "%s", "%s", "%s", "%s")' % \
                                  (result.Data[i * 2],
                                   key_1,
                                   key_2,
                                   str(key)[:-3],
                                   item[key],
                                   key)
                        else:
                            midstr = cursor.fetchone()[0] + ('%s,' % cid)
                            sql = 'UPDATE t_stock SET ' \
                                  'dc_category = "%s",' \
                                  'dc_subcategory_1 = "%s",' \
                                  'dc_subcategory_1_code = "%s",' \
                                  'dc_subcategory_2 = "%s",' \
                                  'dc_subcategory_2_code = "%s",' \
                                  'WHERE code = "%s"' % \
                                  (key_1,
                                   key_2,
                                   str(key)[:-3],
                                   item[key],
                                   key,
                                   result.Data[i * 2])
                        cursor.execute(sql)
                        conn.commit()

    # 拉取深沪股票信息 - 概念类
    for item in EMQuantCodesList_Concept:
        for key_1 in item:
            for key in item[key_1]:
                result = EmQuantAPI.c.sector(key, today)
                if result.ErrorCode != 0:
                    showEMQuantErrMessage(result.ErrorCode)
                    continue
                for i in range(len(result.Data) // 2):
                    # 获取市场分类ID
                    res = cursor.execute('SELECT * FROM t_stock_concept_classes where stock_concept_code="%s"' % key)
                    if res != 1:
                        print('Unkown concept code: %s' % key)
                        break
                    cid = cursor.fetchone()[0]
                    # 检查是否已存在该股票数据
                    res = cursor.execute('SELECT concept FROM t_stock where code="%s"' % result.Data[i * 2])
                    if res == 0:
                        sql = 'INSERT INTO t_stock (code, name, concept) VALUES ("%s", "%s", "%s,")' % \
                              (result.Data[i * 2], result.Data[i * 2 + 1], cid)
                    else:
                        midstr = cursor.fetchone()[0] + ('%s,' % cid)
                        sql = 'UPDATE t_stock SET concept = "%s" WHERE code = "%s"' % \
                              (midstr, result.Data[i * 2])
                    cursor.execute(sql)
                    conn.commit()

    # 拉取股票地域等信息
    res = cursor.execute('SELECT code FROM t_stock')
    if res == 0:
        print('Not find any data')
    else:
        code_data = cursor.fetchall()
        for code in code_data:
            # 东财代码,所属地区板块,省份,城市,区县信息,所属东财行业指数,所属东财行业指数代码
            data = EmQuantAPI.c.css(code, "EMCODE,AREA,PROVINCE,CITY,COUNTYINFORMATION,BLEMIND,BLEMINDCODE",
                                    "ClassiFication=1")
            sql = 'UPDATE t_stock SET ' \
                  'dc_code = "%s",' \
                  'area = "%s",' \
                  'province = "%s",' \
                  'city = "%s",' \
                  'subcity = "%s",' \
                  'dc_industry_index = "%s",' \
                  'dc_industry_index_code = "%s"' % (
                      data[0], data[1], data[2], data[3], data[4], data[5], data[6]
                  )
            cursor.execute(sql)
            conn.commit()

    cursor.close()
    conn.close()

    EmQuantAPI.c.stop()
