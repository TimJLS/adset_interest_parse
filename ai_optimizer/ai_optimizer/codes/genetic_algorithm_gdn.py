#!/usr/bin/env python
# coding: utf-8

# In[83]:


import numpy as np
import random
import copy
import matplotlib.pyplot as plt
import math
import datetime
import pandas as pd
import gdn_datacollector
import gdn_db
from googleads import adwords
# sizepop, vardim, MAXGEN, params = 2000, 3, 30, [0.9, 0.5, 0.5]
sizepop, vardim, MAXGEN, params = 1000, 3, 10, [0.9, 0.5, 0.5]
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'cpa': 'cpa_bid',
}
TARGET_INDEX = {
    'cpc': 'clicks',
    'cpa': 'conversions',
}


class GeneticAlgorithm(object):
    '''
    The class for genetic algorithm
    '''

    def __init__(self, sizepop, vardim, bound, MAXGEN, params):
        '''
        sizepop: population sizepop 種群數量
        vardim: dimension of variables 變量維度
        bound: boundaries of variables 變量邊界 -10 10 (最佳化權重上下限)
        MAXGEN: termination condition  迭代次數  1000 (子代代數)
        param: 交叉率, 變異率, alpha = [0.9, 0.1, 0.5]
        '''
        self.sizepop = sizepop
        self.MAXGEN = MAXGEN
        self.vardim = vardim
        self.bound = bound
        self.population = []
        self.fitness = np.zeros((self.sizepop, 1))
        self.trace = np.zeros((self.MAXGEN, 2))
        self.params = params

    def initialize(self):
        '''
        initialize the population
        '''
        for i in range(0, self.sizepop):
            ind = GAIndividual(self.vardim, self.bound)
            ind.generate()
            self.population.append(ind)

    def evaluate(self):
        '''
        evaluation of the population fitnesses
        '''
        for i in range(0, self.sizepop):
            self.population[i].calculateFitness()
            self.fitness[i] = self.population[i].fitness

    def solve(self):
        '''
        evolution process of genetic algorithm
        '''
        self.t = 0
        self.initialize()
        self.evaluate()
        best = np.max(self.fitness)
        bestIndex = np.argmax(self.fitness)
        self.best = copy.deepcopy(self.population[bestIndex])
        self.avefitness = np.mean(self.fitness)
        self.trace[self.t, 0] = (1 - self.best.fitness) / self.best.fitness
        self.trace[self.t, 1] = (1 - self.avefitness) / self.avefitness
        print("Generation %d: optimal function value is: %f; average function value is %f" % (
            self.t, self.trace[self.t, 0], self.trace[self.t, 1]))
        while (self.t < self.MAXGEN - 1):
            self.t += 1
            self.selectionOperation()
            self.crossoverOperation()
            self.mutationOperation()
            self.evaluate()
            best = np.max(self.fitness)
            bestIndex = np.argmax(self.fitness)
            if best > self.best.fitness:
                self.best = copy.deepcopy(self.population[bestIndex])
            self.avefitness = np.mean(self.fitness)
#             self.trace[self.t, 0] = (1 - self.best.fitness) / self.best.fitness
#             self.trace[self.t, 1] = (1 - self.avefitness) / self.avefitness
            self.trace[self.t, 0] = self.best.fitness
            self.trace[self.t, 1] = self.avefitness
            print("Generation %d: optimal function value is: %f; average function value is %f" % (
                self.t, self.trace[self.t, 0], self.trace[self.t, 1]))

        print("Optimal function value is: %f; " %
              self.trace[self.t, 0])
        print("Optimal solution is:")
        print(self.best.chrom)
        self.printResult()
        return self.best.chrom

    def selectionOperation(self):
        '''
        selection operation for Genetic Algorithm
        '''
        newpop = []
        totalFitness = np.sum(self.fitness)
        accuFitness = np.zeros((self.sizepop, 1))

        sum1 = 0.
        for i in range(0, self.sizepop):
            accuFitness[i] = sum1 + self.fitness[i] / totalFitness
            sum1 = accuFitness[i]

        for i in range(0, self.sizepop):
            r = random.random()
            idx = 0
            for j in range(0, self.sizepop - 1):
                if j == 0 and r < accuFitness[j]:
                    idx = 0
                    break
                elif r >= accuFitness[j] and r < accuFitness[j + 1]:
                    idx = j + 1
                    break
            newpop.append(self.population[idx])
        self.population = newpop

    def crossoverOperation(self):
        '''
        crossover operation for genetic algorithm
        '''
        newpop = []
        for i in range(0, self.sizepop, 2):
            idx1 = random.randint(0, self.sizepop - 1)
            idx2 = random.randint(0, self.sizepop - 1)
            while idx2 == idx1:
                idx2 = random.randint(0, self.sizepop - 1)
            newpop.append(copy.deepcopy(self.population[idx1]))
            newpop.append(copy.deepcopy(self.population[idx2]))
            r = random.random()
            if r < self.params[0]:
                crossPos = random.randint(1, self.vardim - 1)
                for j in range(crossPos, self.vardim):
                    newpop[i].chrom[j] = newpop[i].chrom[
                        j] * self.params[2] + (1 - self.params[2]) * newpop[i + 1].chrom[j]
                    newpop[i + 1].chrom[j] = newpop[i + 1].chrom[j] * self.params[2] +                         (1 - self.params[2]) * newpop[i].chrom[j]
        self.population = newpop

    def mutationOperation(self):
        '''
        mutation operation for genetic algorithm
        '''
        newpop = []
        for i in range(0, self.sizepop):
            newpop.append(copy.deepcopy(self.population[i]))
            r = random.random()
            if r < self.params[1]:
                mutatePos = random.randint(0, self.vardim - 1)
                theta = random.random()
                if theta > 0.5:
                    newpop[i].chrom[mutatePos] = newpop[i].chrom[
                        mutatePos] - (newpop[i].chrom[mutatePos] - self.bound[0, mutatePos]) * (1 - random.random() ** (1 - self.t / self.MAXGEN))
                else:
                    newpop[i].chrom[mutatePos] = newpop[i].chrom[
                        mutatePos] + (self.bound[1, mutatePos] - newpop[i].chrom[mutatePos]) * (1 - random.random() ** (1 - self.t / self.MAXGEN))
        self.population = newpop

    def printResult(self):
        '''
        plot the result of the genetic algorithm
        '''
        x = np.arange(0, self.MAXGEN)
        y1 = self.trace[:, 0]
        y2 = self.trace[:, 1]
        plt.plot(x, y1, 'r', label='optimal value')
        plt.plot(x, y2, 'g', label='average value')
        plt.xlabel("Iteration")
        plt.ylabel("function value")
        plt.title("Genetic algorithm for function optimization")
        plt.legend()
        plt.show()


class GAIndividual(object):
    '''
    individual of genetic algorithm
    '''

    def __init__(self,  vardim, bound):
        '''
        vardim: dimension of variables
        bound: boundaries of variables
        '''
        self.vardim = vardim
        self.bound = bound
        self.fitness = 0.

    def generate(self):
        '''
        generate a random chromsome for genetic algorithm
        '''
        len = self.vardim
        rnd = np.random.random(size=len)
        self.chrom = np.zeros(len)
        for i in range(0, len):
            self.chrom[i] = self.bound[0, i] +                 (self.bound[1, i] - self.bound[0, i]) * rnd[i]

    def calculateFitness(self):
        '''
        calculate the fitness of the chromsome
        '''
        self.fitness = ObjectiveFunc.fitnessfunc(self.chrom, df)


class ObjectiveFunc(object):
    '''
    objective function of genetic algorithm
    '''

    def __init__(self):
        self.DATABASE = 'dev_gdn'
        self.mydb = gdn_db.connectDB(self.DATABASE)
        self.AUTH_FILE_PATH = '/home/tim_su/ai_optimizer/opt/ai_optimizer/googleads.yaml'
        self.client = adwords.AdWordsClient.LoadFromStorage(
            self.AUTH_FILE_PATH)

    def fitnessfunc(optimal_weight, df):
        m_kpi = ( df['campaign_target'] / df['daily_destination'] ).apply(np.tanh)#0.8
        m_spend = ( (df['budget_per_day'] - df['spend']) / df['budget_per_day'] ).apply(np.exp) #0.112
        m_bid = ( (df['campaign_bid'] - df['campaign_cpc']) / df['campaign_bid'] ).apply(np.exp) #-0.29

        status = np.array([m_kpi, m_spend, m_bid])
#         print(df['campaign_bid'].iloc[0], df['campaign_cpc'].iloc[0], df['campaign_bid'].iloc[0])
#         status  = np.array( [m_kpi, m_spend, m_bid, m_width] )
        r = np.dot(optimal_weight, status)
        return r

    def adgroup_fitness(optimal_weight, df):
        m_kpi = ( df['target'] / df['daily_destination'] ).apply(np.tanh) * 10
        if df['target'].iloc[0] == 0:
            m_kpi = -10
        m_spend = ( (df['daily_budget'] - df['spend']) / df['daily_budget'] ).apply(np.exp)
        m_bid = ((df['bid_amount'] - df['cost_per_target']) / df['bid_amount']).apply(np.exp)
        status = np.array([m_kpi, m_spend, m_bid])
        optimal_weight = np.array([
            optimal_weight['weight_kpi'].iloc[0],
            optimal_weight['weight_spend'].iloc[0],
            optimal_weight['weight_bid'].iloc[0]
        ])
        r = np.dot(optimal_weight, status)
#         print(status)
#         print(optimal_weight)
        return r

    def campaign_status(self, campaign_id):
        df_camp = pd.read_sql(
            "SELECT * FROM campaign_target WHERE campaign_id=%s" % (campaign_id), con=self.mydb)
        gdn_datacollector.Campaign(
            df_camp['customer_id'].iloc[0],
            campaign_id,
            df_camp['destination_type'].iloc[0]).get_campaign_insights(client=self.client, date_preset='YESTERDAY')

        daily_destination = (df_camp['destination']/df_camp['period']).iloc[0]
        campaign_bid = (df_camp['daily_budget'] /
                        (df_camp['destination']/df_camp['period'])).iloc[0]
        daily_budget = df_camp['daily_budget'].iloc[0]
        spend = df_camp['spend'].iloc[0]
        campaign_cpc = df_camp['cost_per_target'].iloc[0]
        campaign_target = df_camp['target'].iloc[0]
        impressions = df_camp['impressions'].iloc[0]
        df = pd.DataFrame(
            {
                'campaign_id': [campaign_id],
                'campaign_cpc': [campaign_cpc],
                'campaign_target': [campaign_target],
                'impressions': [impressions],
                'campaign_bid': [campaign_bid],
                'spend': [spend],
                'daily_budget': [daily_budget],
                'daily_destination': [daily_destination],
                'budget_per_day': [daily_budget]
            }
        )
        df = df.convert_objects(convert_numeric=True)
        self.mydb.close()
        return df

    def adgroup_status(self, adgroup_id):
        df = pd.DataFrame({'adgroup_id': [], 'target': [],
                           'impressions': [], 'bid_amount': []})
        df_adgroup = pd.read_sql(
            "SELECT * FROM adgroup_insights WHERE adgroup_id=%s ORDER BY request_time DESC LIMIT 1" % (adgroup_id), con=self.mydb)
        df_camp = pd.read_sql("SELECT * FROM campaign_target WHERE campaign_id=%s" %
                              (df_adgroup['campaign_id'].iloc[0]), con=self.mydb)
        df_adgroup['daily_budget'] = df_camp['daily_budget']
        df_adgroup['bid_amount'] = df_adgroup[ BIDDING_INDEX[df_adgroup['bidding_type'].iloc[0]] ]
        df_adgroup['target'] = df_adgroup[ TARGET_INDEX[df_adgroup['bidding_type'].iloc[0]] ]
        df_camp['daily_destination'] = df_camp['destination'] / df_camp['period']
        df_temp = pd.merge(
            df_adgroup[['campaign_id', 'adgroup_id', 'daily_budget', 'impressions']],
            df_adgroup[['adgroup_id', 'spend', 'bid_amount',
                        'target', 'cost_per_target']], on=['adgroup_id']
        )
        df_status = pd.merge(
            df_temp,
            df_camp[['campaign_id', 'daily_destination']], on=['campaign_id']
        )
#         df_status = df_temp
        df = pd.concat([df, df_status], ignore_index=True, sort=True)
        self.mydb.close()
        return df


def ga_optimal_weight(campaign_id):
    request_time = datetime.datetime.now().date()
    mydb = gdn_db.connectDB("dev_gdn")
    df_weight = pd.read_sql(
        "SELECT * FROM optimal_weight WHERE campaign_id=%s " % (campaign_id), con=mydb)
    df_camp = pd.read_sql(
        "SELECT * FROM campaign_target WHERE campaign_id=%s " % (campaign_id), con=mydb)
    df_adgroup = pd.read_sql(
        "SELECT * FROM adgroup_initial_bid WHERE campaign_id=%s " % (campaign_id), con=mydb)
    destination_type = df_camp['destination_type'].iloc[0]
    adgroup_list = df_adgroup['adgroup_id'].unique()
    for adgroup_id in adgroup_list:

        #         print(adgroup_id)
        df = ObjectiveFunc().adgroup_status(adgroup_id)
        r = ObjectiveFunc.adgroup_fitness(df_weight, df)
#         print('[score]', r, adgroup_id)

        df_final = pd.DataFrame({'campaign_id': campaign_id, 'adgroup_id': adgroup_id,
                                 'score': r, 'request_time': request_time}, index=[0])
        print(adgroup_id, df_final['score'].iloc[0])
        gdn_db.into_table(df_final, table="adgroup_score")
#         try:
# #             print(adgroup_id)
#             df = ObjectiveFunc.adgroup_status(adgroup_id)
#             r = ObjectiveFunc.adgroup_fitness( df_weight, df )
#             print('[score]', r)
#             df_ad=pd.read_sql("SELECT adgroup_id FROM ad_insights WHERE adgroup_id=%s LIMIT 1" %(adgroup_id), con=self.mydb)
#             adgroup_id = df_ad['adgroup_id'].iloc[0].astype(dtype=object)

#             df_final = pd.DataFrame({'campaign_id':campaign_id, 'adgroup_id':adgroup_id, 'adgroup_id':adgroup_id, 'score':r, 'request_time':request_time}, index=[0])

#             gdn_db.intoDB("adgroup_score", df_final)
#         except:
#             pass
    mydb.close()
    return


# In[85]:


if __name__ == "__main__":
    starttime = datetime.datetime.now()

    campaign_id_list = gdn_db.get_campaign()['campaign_id'].unique()
    for campaign_id in campaign_id_list:
#         ga_optimal_weight(campaign_id)
        print('campaign_id:', campaign_id )
        global df
        df = ObjectiveFunc().campaign_status(campaign_id)
        bound = np.tile([[0], [1]], vardim)
        ga = GeneticAlgorithm(sizepop, vardim, bound, MAXGEN, params)
        optimal = ga.solve()
        score = ObjectiveFunc.fitnessfunc(optimal, df)

        score_columns=['weight_kpi', 'weight_spend', 'weight_bid']
        df_score = pd.DataFrame(data=[optimal], columns=['weight_kpi', 'weight_spend', 'weight_bid'], index=[0])
#         score_columns=['weight_kpi', 'weight_spend', 'weight_bid', 'weight_width']
#         df_score = pd.DataFrame(data=[optimal], columns=['weight_kpi', 'weight_spend', 'weight_bid', 'weight_width'], index=[0])        

        df_final = pd.DataFrame({'campaign_id':campaign_id, 'score':score}, columns=['campaign_id', 'score'], index=[0])
        df_final = pd.concat( [df_score, df_final], axis=1, sort=True, ignore_index=False)
        
        print(df_final)
        gdn_db.check_optimal_weight(campaign_id, df_final)
        ga_optimal_weight(campaign_id)
        
        print('optimal_weight:', optimal)
        print(datetime.datetime.now()-starttime)    
    print(datetime.datetime.now()-starttime)
    import gc
    gc.collect()    


# In[ ]:




