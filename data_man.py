# -*- coding: utf-8 -*-
import sqlite3
import time
import os.path
import numpy as np
import pickle
# import pickle
class DataMan():
    '''
    formate data,
    '''
    def __init__(self,userfile):
        self.userfile = userfile
        #self.categorys = 'categorys.txt'
        self.conn = sqlite3.connect("tianchi.db")
        self.cur = self.conn.cursor()
        create_sql = ('CREATE TABLE if not exists tianchi('
                      'id integer primary key autoincrement,'
                      'user_id integer,'
                      'item_id integer,'
                      'behavior_type integer,'
                      'user_geohash TEXT,'
                      'item_category integer,'
                      'time TEXT)')
        self.conn.execute(create_sql)
        self.conn.commit()
    
    def __del__(self):
        self.cur.close()
        self.conn.close()
        
    def to_sqlite(self):
        '''
    import csv data to sqlite3 store in the same location
    total record=12312542,total user=10000;total category=8898,
    total item=2914411
        '''
        files = open(self.userfile)
        #categorys = open(self.categorys,'w')
        files.readline()
        line=files.readline()
        counter = 0
        cur = self.conn.cursor()
        while line:
            #print line
            user_id,item_id,behavior_type,user_geohash,item_category,time = \
            line.split(',')
            #cur = self.conn.cursor()
            sql = 'insert into tianchi(user_id,item_id,behavior_type,user_geohash,item_category,time) '\
                    'values('+user_id+','+item_id+','+behavior_type+',"'+user_geohash+'",'+item_category+',"'+time+'")'
            if counter % 10000==0:
                print counter,'\n',sql
                ##avoiding duo shiwu
                self.conn.commit()
            cur.execute(sql)
            
            #print item_category+'\n'
            line = files.readline()
            counter+=1
        ##last one to commit
        self.conn.commit()
        print 'done'    
        #cur.close()
        #self.conn.close()
        
    def to_sqlite_predict(self,name):
        '''
        import csv data to sqlite3 store in the same location
        record=445623,category=953,item=314694
        '''
        create_sql = ('CREATE TABLE if not exists prediction('
                      'id integer primary key autoincrement,'
                      'item_id integer,'
                      'user_geohash TEXT,'
                      'item_category integer)'
                      )
        self.conn.execute(create_sql)
        self.conn.commit()
        
        
        files = open(name)
        #categorys = open(self.categorys,'w')
        files.readline()
        line=files.readline()
        counter = 0
        while line:
            #print line
            item_id,user_geohash,item_category = \
            line.split(',')
            cur = self.conn.cursor()
            sql = 'insert into prediction(item_id,user_geohash,item_category) '\
                    'values(%s,"%s",%s)' % (item_id,user_geohash,item_category)
            if counter % 10000==0:
                print counter,'\n',sql
                ##avoiding duo shiwu
                self.conn.commit()
            cur.execute(sql)
            
            #print item_category+'\n'
            line = files.readline()
            counter+=1
        ##last one to commit
        self.conn.commit()
        print 'done'    
        #cur.close()
        #self.conn.close()
    def get_by_category(self, category):
        '''
        根据类别获取数据
        '''
        #start = time.time()
        ##-------------------------------------------------------------------------
        name = 'Data/user_'+str(category)+'.dt'
        if os.path.exists(name):
            f = open(name,'r')
            user_res1=pickle.load(f)
            f.close()
        else:
            ##--------user set--------
            user_sql = 'select distinct user_id from tianchi '\
            'where item_category=%s' % str(category)
            self.cur.execute(user_sql)
            user_res = self.cur.fetchone()
    
            user_res1 = []
            while user_res:
                user_res1.append(user_res[0])
                user_res = self.cur.fetchone()
            f = open(name,'w')
            pickle.dump(user_res1,f)
            f.close()
        
        name = 'Data/item_'+str(category)+'.dt'
        if os.path.exists(name):
            f = open(name,'r')
            item_res1=pickle.load(f)
            f.close()
        else:
            item_sql = 'select distinct item_id from tianchi '\
            'where item_category=%s' % str(category)
            self.cur.execute(item_sql)
            item_res = self.cur.fetchone()
            item_res1 = []
    
            while item_res:
                item_res1.append(item_res[0])
                item_res = self.cur.fetchone()
            f = open(name,'w')
            pickle.dump(item_res1,f)
            f.close()
        
        ###类别过大####
        if len(item_res1)>5000 or len(user_res1)>5000:
            print "User:%d Item:%d is to big to compute" %(len(user_res1),len(item_res1))
            return user_res1,item_res1,[]
        ##存在数据####
        if os.path.exists('Data/'+str(category)+'.npy'):
            return user_res1,item_res1,[1]#区别返回数据[(x,x,x,x)]

        catsetall = []
        for user in user_res1:
            items_set = []
            for item in item_res1:

                item_set = [0, 0, 0, 0]
                items_set.append(item_set)
            
            catsetall.append(items_set) 
        #-----------category-----------
        
        category_sql = 'select user_id,item_id,behavior_type from tianchi '\
        'where item_category=%s' % str(category)
        self.cur.execute(category_sql)
        cat_res = self.cur.fetchone()
        while cat_res: #setall[user_id][item_id][behavior]+= behavior_type
            catsetall[user_res1.index(cat_res[0])][item_res1.index(cat_res[1])][cat_res[2] - 1] += cat_res[2]
            cat_res = self.cur.fetchone()

        #print "Get current category matrix use:%s s" % (time.time() - start)

        
        return user_res1,item_res1,catsetall           
    def get_prediction_category(self):
        '''
        get score matrix by category
        '''  
        #####---------all category
        sql = 'select distinct item_category from prediction'
        #cur = self.conn.cursor()       
        self.cur.execute(sql)
        res = self.cur.fetchone()
        categorys = []
        while res:
            categorys.append(res[0])
            res = self.cur.fetchone()
   
        return categorys
    def get_pitem_by_cat(self,cat):
        sql = 'select distinct item_id from prediction where item_category=%s' % str(cat)
        self.cur.execute(sql)
        res = self.cur.fetchone()
        items = []
        while res:
            #print res[0]
            items.append(res[0])
            res = self.cur.fetchone()
        return items        
    def single_csv(self,fname,f2name):   
        '''
        remove repeate item,use set & set &set &...
        merge two file to one
        '''
        fhand = open(fname,'r')
        content = fhand.readlines()
        title = content[0]
        content = set(content[1:])
        
        f2hand = open(f2name,'r')
        content2 = f2hand.readlines()
        content2 = set(content2[1:])
        
        tofile = open('MergeResults.csv','w')
        tofile.write(title)
        for line in content | content2:
            tofile.write(line)
            
        fhand.close()
        f2hand.close()
        tofile.close()
        print 'Generate Complate~'
        
    def mergecsv(self,his,categorys):
        '''
        合并零散csv文件
        '''
        if his==0:return
        
        title = 'user_id,item_id\n'
        tocsv = open('results.csv','w')
        tocsv.write(title)
         
        results = set()
        for h in range(his):
            try:
                filename = 'Products/results_'+str(categorys[h])+'.csv'
                filehand = open(filename,'r')
                content = filehand.readlines()
                if len(content)==0:continue
                content = set(content[1:])
                results = results | content
                filehand.close()
            except:
                filename = 'Products/results_'+str(categorys[h])+'.csv'
                print filename+' does not exists'
                continue
        
        for line in results:
            tocsv.write(line)
        tocsv.close()  
        print 'Merge complete'
        
if __name__=='__main__':
    import os
    filename = 'tianchi_mobile_recommend_train_user.csv'
    dataman = DataMan(filename) 
    #dataman.to_sqlite() 
    dataman.to_sqlite_predict('prediction.csv') 
    #dataman.store_user_category()
    #dataman.formate_data()
    #dataman.get_prediction_bycategory()
    #dataman.single_csv('results.csv')
    #dataman.get_score_bycategory()
