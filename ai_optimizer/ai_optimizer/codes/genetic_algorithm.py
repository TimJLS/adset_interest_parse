#!/usr/bin/env python
# coding: utf-8

# In[1]:


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random
import copy
import math
import datetime
import adgeek_permission as permission
import facebook_datacollector as collector
import database_controller
sizepop, vardim, MAXGEN, params = 1000, 7, 15, [0.9, 0.5, 0.5]
BRANDING_CAMPAIGN_LIST = [
    'THRUPLAY', 'LINK_CLICKS', 'ALL_CLICKS','VIDEO_VIEWS', 'REACH', 'IMPRESSIONS', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
PERFORMANCE_CAMPAIGN_LIST = [
    'CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']


# In[2]:


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
        print ("Optimal solution is:")
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
        optimal_weight = OptimalWeight(self.chrom)
        self.fitness = np.dot(optimal_weight.matrix, chromosome.matrix)
#         self.fitness = ObjectiveFunc.fitnessfunc( self.chrom, df )


# def ga_optimal_weight(campaign_id):
#     request_time = datetime.datetime.now().date()
#     mydb = mysql_saver.connectDB( mysql_saver.DATABASE )
#     df_weight = pd.read_sql("SELECT * FROM optimal_weight WHERE campaign_id=%s " %(campaign_id), con=mydb)
#     df_camp = pd.read_sql("SELECT * FROM campaign_target WHERE campaign_id=%s " %(campaign_id), con=mydb)
#     charge_type = df_camp['charge_type'].iloc[0]
#     adset_list = collector.Campaigns(campaign_id, charge_type).get_adsets_active()
#     if len(df_weight) > 0:
#         for adset_id in adset_list:
#             df = ObjectiveFunc.adset_status( adset_id, df_camp )
#             r = ObjectiveFunc.adset_fitness( df_weight, df )
#             print('[score]', r)
#             df_final = pd.DataFrame({'campaign_id':campaign_id, 'adset_id':adset_id, 'score':r, 'request_time':request_time}, index=[0])
#             try:
#                 mysql_saver.intoDB("adset_score", df_final)
#             except Exception as e:
#                 print('score is inf: ', e)
#                 pass
#     else:
#         pass
#     mydb.close()
#     return


# In[3]:


def generate_optimal_weight(campaign_id, charge_type):
    global chromosome
    print('[campaign_id]:', campaign_id )
    print('[current time]: ', datetime.datetime.now() )
    start_time = datetime.datetime.now()
    
    campaign = Campaign(campaign_id)
    chromosome = ObjectChromosome(campaign.condition)

    bound = np.tile([[0], [1]], vardim)
    ga = GeneticAlgorithm(sizepop, vardim, bound, MAXGEN, params)
    result_optimal_weight = ga.solve()
    
    optimal_campaign = CampaignOptimalWeight(campaign_id, charge_type, result_optimal_weight)
    
    score = np.dot(optimal_campaign.matrix, chromosome.matrix)
    print('==========SCORE========')
    print(score)

    score_columns=['w_action', 'w_desire', 'w_interest', 'w_awareness', 'w_discovery', 'w_spend', 'w_bid']
    df_score = pd.DataFrame(data=[optimal_campaign.matrix], columns=score_columns, index=[0])
    df_score['campaign_id'], df_score['score'] = campaign_id, score

    database_fb.upsert("campaign_optimal_weight", df_score.to_dict('records')[0])

    assess_adset(campaign, optimal_campaign)

    print('[optimal_weight]:', optimal_campaign.matrix)
    print('[operation time]: ', datetime.datetime.now()-start_time)


# In[4]:


def assess_adset(campaign_object, campaign_optimal_object):
    for adset in campaign_object.adsets:
        
        chromosome_adset = ObjectChromosome(adset.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_adset.matrix)
        
        print('=====[adset_id]=====', adset.adset_id, '==========[score]', score)

        database_fb.insert(
            "adset_score", 
            {'campaign_id':campaign_object.campaign_id, 'adset_id':adset.adset_id, 'score':score.item()}
        )


# In[5]:


def main():
    starttime = datetime.datetime.now()
    print('[start time]: ', starttime)
    global database_fb
    db = database_controller.Database()
    database_fb = database_controller.FB(db)
    campaign_list = database_fb.get_branding_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_list])
    for campaign in campaign_list:
        campaign_id = campaign.get("campaign_id")
        charge_type = campaign.get("charge_type")
        generate_optimal_weight(campaign_id, charge_type)
    
    campaign_list = database_fb.get_performance_campaign().to_dict('records')
    for campaign in campaign_list:
        campaign_id = campaign.get("campaign_id")
        charge_type = campaign.get("charge_type")
        generate_optimal_weight(campaign_id, charge_type)
        
    print('[total operation time]: ', datetime.datetime.now()-starttime)
    print('genetic algorithm finish.')
    return


# In[6]:


class Campaign(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "discovery", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.__get_brief()
        self.brief_dict["destination_type"] = self.brief_dict.pop("charge_type")
        self.destination_type = self.brief_dict.get("destination_type")
        permission.init_facebook_api(self.brief_dict.get("account_id"))
        self.service_collect = collector.Campaigns(self.campaign_id, charge_type=None)
        self.__create_condition()
        self.__get_adsets()
        
    def __get_brief(self):
        df_list = database_fb.get_one_campaign(self.campaign_id).to_dict('records')

        self.brief_dict = df_list[0]
        self.brief_dict['KPI'] = self.brief_dict.get("ai_spend_cap")/self.brief_dict.get("destination")
    
    def get_weight(self):
        optimal_weight_list = database_fb.retrieve("optimal_weight", self.campaign_id).to_dict('records')
        
        return optimal_weight_list[0]
        
    def __get_adsets(self):
        adset_id_list = self.service_collect.get_adsets_active()
        self.adsets = [ AdSet(self, adset_id) for adset_id in adset_id_list ]
        
    def __create_condition(self):
        self.condition = self.service_collect.generate_info(date_preset = collector.DatePreset.lifetime)
        self.condition.update(self.brief_dict)
        self.condition.update({
            "flight": (datetime.date.today()-self.brief_dict.get("ai_start_date")).days
        })
        self.condition['spend'] = int(self.condition.get("spend", 0))
        self.condition['impressions'] = int(self.condition.get("impressions", 0))
        self.condition.update({
            "attention": self.condition.get("impressions"),
            "discovery": self.condition.get("reach"),
            "destination_daily_spend": self.condition.get("ai_spend_cap") / self.condition.get("period"),
            "destination_daily_target":self.condition.get("destination") / self.condition.get("period"),
            "cost_per_action": self.condition.get("cost_per_target", 0),
            "spend": self.condition.get("spend") / self.condition.get("flight") if self.condition.get("flight") != 0 else 1,
            "action": self.condition.get("target") / self.condition.get("flight") if self.condition.get("flight") != 0 else 1,
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}
        
        
class AdSet(object):
    __condition_field = ["action", "desire", "interest", "awareness", "impressions", "destination_daily_spend",
                         "destination_daily_target", "bid_amount", "cost_per_action", "spend", "KPI", "destination_type"]
    def __init__(self, campaign, adset_id):
        self.campaign = campaign
        self.adset_id = adset_id
        self.destination_type = self.campaign.destination_type
        self.service_collect = collector.AdSets(self.adset_id, charge_type=None)
        self.__create_condition()
        
    def __create_condition(self):
        self.condition = self.service_collect.generate_info(date_preset = collector.DatePreset.lifetime)
        self.condition['spend'] = float(self.condition.get("spend", 0))
        self.condition['impressions'] = int(self.condition.get("impressions", 0))
        self.condition.update({
            "KPI": self.campaign.condition.get("KPI"),
            "destination_type": self.destination_type,
            "destination_daily_spend": self.campaign.condition.get("destination_daily_spend"),
            "destination_daily_target": self.campaign.condition.get("destination_daily_target"),
            "cost_per_action": int(self.condition.get("spend")) / int(self.condition.get("action")) if int(self.condition.get("action")) != 0 else 0
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}
        
    def __create_fitness(self):
        self.fitness = Chromosome(self.condition)
        
    def __create_optimal_weight(self):
        self.optimal_weight = OptimalWeight(self.destination_type)


# In[7]:


class OptimalWeight(object):        
    def __init__(self, optimal_weight):
        self.matrix = optimal_weight
        self.action = self.matrix[0]
        self.desire = self.matrix[1]
        self.interest = self.matrix[2]
        self.awareness = self.matrix[3]
        self.discovery = self.matrix[4]
        self.spend = self.matrix[5]
        self.kpi = self.matrix[6]

class CampaignOptimalWeight(OptimalWeight):
    def __init__(self, campaign_id, destination_type, optimal_weight):
        super().__init__(optimal_weight)
        self.destination_type = destination_type
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.desire, self.interest, self.awareness, self.discovery = 0, 0, 0, 0
            self.matrix = np.array([
                self.action, self.desire, self.interest, self.awareness, self.discovery, self.spend, self.kpi
            ])

    


# In[8]:


class Chromosome(object):
    def __init__(self,):
        self.matrix = np.random.rand(vardim,)
        self.action = self.matrix[0]
        self.desire = self.matrix[1]
        self.interest = self.matrix[2]
        self.awareness = self.matrix[3]
        self.discovery = self.matrix[4]
        self.spend = self.matrix[5]
        self.kpi = self.matrix[6]

class RandomChromosome(Chromosome):
    pass

class ObjectChromosome(Chromosome):
    __fields = [
        "action", "desire", "interest", "awareness", "discovery", "attention", "spend"
    ]
    def __init__(self, condition):
        super().__init__()
        self.condition = condition
        self.destination_type = condition.get("destination_type")
        self.__initialize()
        self.__create_m_action()
        self.__create_m_desire()
        self.__create_m_interest()
        self.__create_m_awareness()
        self.__create_m_discovery()
        self.__create_m_spend()
        self.__create_m_kpi()
        self.__create_matrix()
        
    def __initialize(self):
        for field in self.__fields:
            if not self.condition.get(field):
                self.condition[field] = 0
        
    def __create_m_action(self):
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.m_action = (self.condition.get("destination_daily_target")/int(self.condition.get("action"))) if int(self.condition.get("action")) != 0 else 0
        else:
            self.m_action = (int(self.condition.get("action")) / self.condition.get("desire")) if int(self.condition.get("desire")) != 0 else 0
        
    def __create_m_desire(self):
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.m_desire = 0
        else:
            self.m_desire = (self.condition.get("desire") / self.condition.get("interest")) if self.condition.get("interest") != 0 else 0
        
    def __create_m_interest(self):
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.m_interest = 0
        else:
            self.m_interest = (self.condition.get("interest") / self.condition.get("awareness")) if self.condition.get("awareness") != 0 else 0
        
    def __create_m_awareness(self):
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.m_awareness = 0
        else:
            self.m_awareness = (self.condition.get("awareness") / self.condition.get("discovery")) if self.condition.get("discovery") != 0 else 0
        
    def __create_m_discovery(self):
        if self.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.m_discovery = 0
        else:
            self.m_discovery = (self.condition.get("discovery") / self.condition.get("attention")) if self.condition.get("attention") != 0 else 0
        
    def __create_m_spend(self):
        self.m_spend = ( self.condition.get("destination_daily_spend")-self.condition.get("spend")) / self.condition.get("destination_daily_spend")
        
    def __create_m_kpi(self):
        self.m_kpi = ( self.condition.get("KPI")-self.condition.get("cost_per_action") ) / self.condition.get("KPI")
    
    def __create_matrix(self):
        self.matrix = np.array([
            self.m_action, self.m_desire, self.m_interest, self.m_awareness, self.m_discovery, self.m_spend, self.m_kpi
        ])
    


# In[9]:


if __name__ == "__main__":
    main()
    import gc
    gc.collect()
#     main(campaign_id=23843467729120098)


# In[10]:


# !jupyter nbconvert --to script genetic_algorithm.ipynb


# In[ ]:




