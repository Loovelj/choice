# python 3.7.4
# coding = utf-8
# filename utils.py
# author 463714869@qq.com/www.cdzcit.com,
#        create by VIM at 2019/12/30

from datasets import EMQuantErrDict


def showEMQuantErrMessage(errcode):
    errMessage = EMQuantErrDict.get(errcode, "Unkown error code-未知的错误代码")
    print('%s - %s' % (errcode, errMessage))
