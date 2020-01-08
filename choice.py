# python 3.7.4
# coding = utf-8
# filename choice.py
# author 463714869@qq.com/www.cdzcit.com,
#        create by VIM at 2019/12/30
from datasets import EMQuantErrDict
from EMQuantAPI import EmQuantAPI


def showEMQuantErrMessage(errcode):
    errMessage = EMQuantErrDict.get(errcode, "Unkown error code-未知的错误代码")
    print('%s - %s' % (errcode, errMessage))


def choiceLogin():
    loginResult = EmQuantAPI.c.start()
    if loginResult.ErrorCode != 0:
        showEMQuantErrMessage(loginResult.ErrorCode)
        exit()
