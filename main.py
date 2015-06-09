#coding: utf-8
import time
import sys
import os.path
import numpy as np
from data_man import DataMan
from SVDMan import SVDMan

##-------------------global variable----------------
##读取历史
try:
    hish = open('Products/history.txt','r+')
    his = len(hish.readlines())
    logfile = open('Products/log.txt','a')
except:
    hish = open('Products/history.txt','w')
    logfile = open('Products/log.txt','w')
    his = 0
##------------------------------------------------   
def main(start=0):
    filename = 'tianchi_mobile_recommend_train_user.csv'
    dataman = DataMan(filename)
    
    
    #预测集类别
    categorys = dataman.get_prediction_category()
    ##计数器
    counter = 0
    
    #print his---------------------------------
    if start==0:
        categorys = categorys[:100]
    elif start==100:
        categorys = categorys[100:200]
    elif start==200:
        categorys = categorys[200:300]
    elif start==300:#####3
        categorys = categorys[300:500]
    elif start==500:
        categorys = categorys[500:]
    
    for cat in categorys:
    ##------分页部分----------------------------------
        ##############
        #cat = 30
        ################
        svdman = SVDMan(cat) 
        if counter<his:#已经算过
            counter+=1
        else:     
            svdman.start(logfile)
             
            counter+=1
            hish.write(str(counter)+'\n')
            hish.flush()
        #####################
        #break
        #####################    
    hish.close()
    logfile.close()

def bigcat():
    filename = 'tianchi_mobile_recommend_train_user.csv'
    dataman = DataMan(filename)
    svdman = SVDMan()
    ##------------------处理大类别----------------------------------------------------
    logfile = open('Products/log.txt','r')
    for line in logfile.readlines():
        print '------category:'+line[:-1]+'------'
        svdman.rec_big_category(dataman, int(line[:-1]))
    sys.exit(0)
    ##--------------------------------------------------------
def tocsv(start,his,mode=0):
    filename = 'tianchi_mobile_recommend_train_user.csv'
    dataman = DataMan(filename)
    #预测集类别
    categorys = dataman.get_prediction_category()
    if start==0:
        categorys = categorys[:500]
    elif start==500:
        categorys = categorys[500:]
    if mode==0:
        ##合并csv文件
        dataman.mergecsv(his,categorys)
    #sys.exit(0)
    else:
        dataman.single_csv('results.csv', 'results1.csv')
    #sys.exit(0)
    ##--------------------------------------------------------------

main(-1)#一天的数据大约在30w左右,购买3k左右
#select count(*) from tianchi where time>'2014-12-05 00' and time<'2014-12-05 23' and behavior_type=4;
tocsv(-1, his,mode=0)#合并全部csv文件
#tocsv(0, his,mode=1)
