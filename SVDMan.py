# -*- coding: utf-8 -*-
from numpy import *
import numpy as np
from numpy import linalg as la
from data_man import DataMan
import time
import os.path
class  SVDMan:
    '''
    svd for project
    '''
    def __init__(self,cat):
        self.items = []#某类别中总商品集
        self.users = []
        self.dataMat = []
        self.xformedItems = []
        self.lower = 6
        self.cat = cat
        self.Simi = []
        
    def loadData(self,dataman,category,mode=0):
        '''
        导入一个列别上数据
        '''
        
        self.users,self.items,self.dataMat = dataman.get_by_category(category)
        ##类别过大,或类别为空
        if self.dataMat==[]:
            return False
        ##-----------from file-------------------##
        if self.dataMat==[1]:
            if mode==0:
                self.dataMat=np.mat(np.load('Data/'+str(category)+'.npy'))
        else:
        ##------------from sql------------------------------------
            for i in range(len(self.users)):
                for j in range(len(self.items)): #每个item 4行
                    rate = 0
                    for behavior in self.dataMat[i][j]:
                        rate += behavior
                    self.dataMat[i][j] = rate
            #需要转置    
            self.dataMat = mat(self.dataMat).T
            ##---保存数据------------#
            np.save('Data/'+str(category)+'.npy',self.dataMat)
        #######SVD ####        
#         U,Sigma,VT = la.svd(self.dataMat)
#         k = self.svdbestk(Sigma)
#         Sigk = mat(eye(k)*Sigma[:k]) #arrange Sig4 into a diagonal matrix
#         self.xformedItems = self.dataMat.T * U[:,:k] * Sigk.I  #create transformed items
#         np.save('Data/'+str(category)+'_svd.npy',self.xformedItems)
        ######----------------------
        print '[+] load init matrix done'
        return True
        
    def cosSim(self,inA,inB):
        num = float(inA.T*inB)
        denom = la.norm(inA)*la.norm(inB)
        return 0.5+0.5*(num/denom)
    
    def ecludSim(self,inA,inB):
        return 1.0/(1.0 + la.norm(inA - inB))
    
    def standEst(self, item, user):
        '''
        最相近k个项目,找到商品对用户的打分
        '''
        simTotal = 0.0; ratSimTotal = 0.0
        ratSimTotal = np.dot(self.dataMat[item,:],self.Simi[:,user])
        ####
        uin = np.nonzero(self.dataMat[item,:].A[0])
        simTotal = np.sum(self.Simi[user,uin])
        ###
        #simTotal = np.sum(self.Simi[user,:])
        #print simTotal
        if simTotal == 0: return 0
        return ratSimTotal/simTotal
        
    def svdEst(self,item,user):
        simTotal = 0.0; ratSimTotal = 0.0
        n = shape(self.dataMat)[1]
        for j in range(n):#over user
            itemRating = self.dataMat[item,j]
            if itemRating == 0 or j==user: continue
            similarity = self.ecludSim(self.xformedItems[user,:].T,\
                                  self.xformedItems[j,:].T)
              
            #print 'the %d and %d similarity is: %f' % (item, j, similarity)
            simTotal += similarity
            ratSimTotal += similarity * itemRating
        if simTotal == 0: return 0
        else: return ratSimTotal/simTotal        
    def recmmandUser(self,item_index,pscores,K=3,mode=0):
        '''
        根据某个商品的index,返回预测评分最高用户
        '''
        userScores = []
        for user_index in range(len(self.users)):
            if mode==0:###存储
                estimatedScore = self.standEst(item_index, user_index)
                ##记录下结果数据
                pscores[item_index,user_index] = estimatedScore
            else:###读取,还原
                estimatedScore = pscores[item_index,user_index]
            
            if estimatedScore>self.lower:
                userScores.append((user_index,estimatedScore))
        n = K
        if len(userScores)<K: n=len(userScores)
        return sorted(userScores,key=lambda d:d[1], reverse = True)[:n]
    
    def calSimMat(self):
        '''
        user similarity
        '''
        name = 'Simi/simi_'+str(self.cat)+'.npy'
        if os.path.exists(name):
            self.Simi=np.mat(np.load(name))
        else:
            self.Simi = np.zeros((len(self.users),len(self.users)),np.float)
            self.Simi = np.mat(self.Simi)
            for i in range(len(self.users)):
                for j in range(i,len(self.users)):
                    if i==j:
                        self.Simi[i,j] = 1.0
                        continue
                    similarity = 0.0
                    overLap = nonzero(logical_and(self.dataMat[:,i].A>0, \
                                          self.dataMat[:,j].A>0))[0]
                    if len(overLap) == 0: 
                        similarity = 0.0
                    else: 
                        similarity = self.ecludSim(self.dataMat[overLap,i], \
                                               self.dataMat[overLap,j])
                    self.Simi[i,j] = similarity
                    self.Simi[j,i] = similarity
            np.save(name,self.Simi)   
        print '[+] SimMat saved'   
        
    def start(self,logfile):
        filename = 'tianchi_mobile_recommend_train_user.csv'
        dataman = DataMan(filename)
        print '----Current category:%s----' % self.cat
        start = time.time()#764s/cat
        #0初始化,1直接使用得分矩阵
        mode = 1
        #处理完,类别太大,跳过
        if not self.loadData(dataman,self.cat,mode): #cat=11991
            logfile.write(str(self.cat)+'\n')
            logfile.flush()
        else:
            if mode==0:
                self.calSimMat()
            ##预测文件--------------------------------------------
            fileh = open('Products/results_'+str(self.cat)+'.csv','w')
            fileh.write('user_id,item_id\n')
            items = dataman.get_pitem_by_cat(self.cat)

            name = 'Score/pscores_'+str(self.cat)+'.dat'
            if os.path.exists(name) and mode!=0:
                pscores=np.memmap(name,mode="c",dtype=np.float32,shape=(len(items),len(self.users)))
            else:
                pscores=np.memmap(name,mode="w+",dtype=np.float32,shape=(len(items),len(self.users)))
            ###user memory matix####
            for item in items:
                #print item_index
                ##相识商品 
                item_index = self.items.index(item)
                
                recusers= self.recmmandUser(item_index,pscores,2,mode)
                #####
                #print recusers
                #####
                for u in recusers:
                    fileh.write(str(self.users[u[0]])+','+str(item)+'\n')  
                #print "Elapsed Time: %s s/1 item" % (time.time() - start)
                #start = time.time()
                ##---------------------
            fileh.close()
            ##--------------------------------------------------
            
        print "Elapsed Time: %s s/1 category" % (time.time() - start)

    def svdbestk(self,Sigma):
        '''
        返回svd最佳参数
        '''
        ption = 0.9
        Sig2=Sigma**2
        tol = sum(Sig2)
        for i in range(len(Sigma)):
            if sum(Sig2[:i])>=tol*ption:
                k = i
                break
        return k   
if __name__=="__main__":
    filename = 'tianchi_mobile_recommend_train_user.csv'
    dataman = DataMan(filename)
    svdman = SVDMan()
    category = 6648
    svdman.rec_big_category(dataman, category)
