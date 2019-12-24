#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
sizepop, vardim, MAXGEN, params = 1000, 7, 40, [0.9, 0.5, 0.5]


# In[ ]:


class GeneticAlgorithm(object):
    '''
    The class for genetic algorithm
    '''
    def __init__(self, genetic_campaign, sizepop, vardim, MAXGEN, params):
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
        self.bound = np.tile([[0], [1]], vardim)
        self.population = []
        self.fitness = np.zeros((self.sizepop, 1))
        self.trace = np.zeros((self.MAXGEN, 2))
        self.params = params
        self.campaign = genetic_campaign

    def initialize(self):
        '''
        initialize the population
        '''
        for i in range(0, self.sizepop):
            ind = GAIndividual(self.vardim, self.bound)
            ind.generate()
            self.population.append(ind)

    def evaluate(self, chromosome_object):
        '''
        evaluation of the population fitnesses
        '''
        for i in range(0, self.sizepop):
            self.population[i].calculate_fitness(self.campaign.chromosome)
            self.fitness[i] = self.population[i].fitness

    def solve(self, time_slice):
        '''
        evolution process of genetic algorithm
        '''
        self.t = 0
        self.initialize()
        if time_slice == collector.DatePreset.lifetime:
            self.evaluate(self.campaign.chromosome)
        elif time_slice == collector.DatePreset.last_7d:
            self.evaluate(self.campaign.chromosome_7d)
        else: raise ValueError("Incorrect 'time_slice' value, should be 'lifetime' or 'last_7d'.")
        best = np.max(self.fitness)
        bestIndex = np.argmax(self.fitness)
        self.best = copy.deepcopy(self.population[bestIndex])
        self.avefitness = np.mean(self.fitness)
        self.trace[self.t, 0] = self.best.fitness#(1 - self.best.fitness) / self.best.fitness
        self.trace[self.t, 1] = self.avefitness#(1 - self.avefitness) / self.avefitness
        print("Generation %d: optimal function value is: %f; average function value is %f" % (
            self.t, self.trace[self.t, 0], self.trace[self.t, 1]))
        while abs(self.trace[self.t, 0] - self.trace[self.t, 1])/self.trace[self.t, 1] > 0.05 and (self.t < self.MAXGEN - 1):
            self.t += 1
            self.selectionOperation()
            self.crossoverOperation()
            self.mutationOperation()
            self.evaluate(self.campaign.chromosome)
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
        self.best_optimal_weight = self.best.chrom
        return CampaignOptimalWeight(self.campaign, self.best_optimal_weight)
    
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
            self.chrom[i] = self.bound[0, i] + (self.bound[1, i] - self.bound[0, i]) * rnd[i]
        
    def calculate_fitness(self, chromosome_object):
        '''calculate the fitness of the chromsome'''
        optimal_weight = OptimalWeight(self.chrom)
        self.fitness = np.dot(optimal_weight.matrix, chromosome_object.matrix)
#         self.fitness = ObjectiveFunc.fitnessfunc( self.chrom, df )


# In[ ]:


def generate_optimal_weight(campaign_id, time_slice):
    table_name = "campaign_optimal_weight" if time_slice == 'lifetime' else "campaign_optimal_weight_7d"
    
    print('==========[campaign_id]:', campaign_id )
    print('==========[current time]: ', datetime.datetime.now())
    start_time = datetime.datetime.now()
    
    campaign = Campaign(campaign_id)
    print('==========RUNNING {}=========='.format(time_slice.upper()))
    campaign.run(time_slice=time_slice)
    print('==========SCORE========')
    print(campaign.score)

    score_columns=['w_action', 'w_desire', 'w_interest', 'w_awareness', 'w_discovery', 'w_spend', 'w_bid']
    df_score = pd.DataFrame(data=[campaign.optimal_weight_object.matrix], columns=score_columns, index=[0])
    df_score['campaign_id'], df_score['score'] = campaign_id, campaign.score
    database_fb.upsert(table_name, df_score.to_dict('records')[0])

#     assess_adset(campaign, optimal_campaign)

    print('[optimal_weight]:', campaign.optimal_weight_object.matrix)
    print('[operation time]: ', datetime.datetime.now()-start_time)


# In[ ]:


def assess_adset(campaign_object, campaign_optimal_object):
    for adset in campaign_object.adsets:
        
        chromosome_adset = ObjectChromosome(adset.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_adset.matrix)
        
        print('=====[adset_id]=====', adset.adset_id, '==========[score]', score)

        database_fb.insert(
            "adset_score", 
            {'campaign_id':campaign_object.campaign_id, 'adset_id':adset.adset_id, 'score':score.item()}
        )


# In[ ]:


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
        for time_slice in ['lifetime', 'last_7d']:
            generate_optimal_weight(campaign_id, time_slice=time_slice)
    
    campaign_list = database_fb.get_performance_campaign().to_dict('records')
    for campaign in campaign_list:
        campaign_id = campaign.get("campaign_id")
        for time_slice in ['lifetime', 'last_7d']:
            generate_optimal_weight(campaign_id, time_slice=time_slice)
        
#     campaign_list = database_fb.get_custom_performance_campaign().to_dict('records')
#     for campaign in campaign_list:
#         campaign_id = campaign.get("campaign_id")
#         charge_type = campaign.get("charge_type")
#         generate_optimal_weight(campaign_id) 
        
    print('[total operation time]: ', datetime.datetime.now()-starttime)
    print('genetic algorithm finish.')
    return


# In[ ]:


class Campaign(object):
    _condition_field = [
        "action", "desire", "interest", "awareness", "attention", "discovery", "impressions", "destination_spend",
        "destination_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"
    ]
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self._get_brief()
        self.destination_type = self.brief_dict.get("destination_type")
        self.service_collect = collector.Campaigns(self.campaign_id, charge_type=None)
        self.condition = self._create_condition(time_slice=collector.DatePreset.lifetime)
        self.condition_7d = self._create_condition(time_slice=collector.DatePreset.last_7d)
        self._get_adsets()
        self.chromosome = ObjectChromosome(self.condition)
        self.chromosome_7d = ObjectChromosome(self.condition_7d)
        self.genetic_algorithm = GeneticAlgorithm(self, sizepop, vardim, MAXGEN, params)
        
    def _get_brief(self):
        self.brief_dict = database_fb.get_brief(self.campaign_id)
        self.brief_dict['KPI'] = self.brief_dict.get("ai_spend_cap")/self.brief_dict.get("destination")
        
    def run(self, time_slice):
        if not time_slice: 
            raise ValueError("time_slice should be 'lifetime' or 'last_7d'.")
        if time_slice == collector.DatePreset.lifetime:
            self.optimal_weight_object = self.genetic_algorithm.solve(time_slice=collector.DatePreset.lifetime)
            self.score = np.dot(self.optimal_weight_object.matrix, self.chromosome.matrix)
            pass
        elif time_slice == collector.DatePreset.last_7d:
            self.optimal_weight_object = self.genetic_algorithm.solve(time_slice=collector.DatePreset.last_7d)
            self.score = np.dot(self.optimal_weight_object.matrix, self.chromosome_7d.matrix)
        else: raise ValueError("Incorrect 'time_slice' value, should be 'lifetime' or 'last_7d'.")
    
    def get_weight(self, time_slice=None):
        if not time_slice or time_slice==collector.DatePreset.lifetime:
            optimal_weight_list = database_fb.retrieve("campaign_optimal_weight", self.campaign_id).to_dict('records')
        elif time_slice==collector.DatePreset.last_7d:
            optimal_weight_list = database_fb.retrieve("campaign_optimal_weight_7d", self.campaign_id).to_dict('records')
        else: raise ValueError("time_slice should be 'lifetime' or 'last_7d'.")
        return optimal_weight_list[0]
        
    def _get_adsets(self):
        adset_id_list = self.service_collect.get_adsets_active()
        self.adsets = [ AdSet(self, adset_id) for adset_id in adset_id_list ]
        
    def _create_condition(self, time_slice=None):
        if not time_slice or time_slice==collector.DatePreset.lifetime:
            condition = self.service_collect.generate_info(date_preset = collector.DatePreset.lifetime)
            time_delta = datetime.date.today()-self.brief_dict.get("ai_start_date")
            flight = (time_delta.days / condition.get("period")) if (time_delta.days!=0) else (1 / condition.get("period"))
        elif time_slice==collector.DatePreset.last_7d:
            condition = self.service_collect.generate_info(date_preset = collector.DatePreset.last_7d)
            flight = (7 / condition.get("period"))
        else: raise ValueError("time_slice should be 'lifetime' or 'last_7d'.")
        condition.update(self.brief_dict)
        condition.update({
            "flight": flight
        })
        condition['spend'] = eval(condition.get("spend", 0))
        condition['impressions'] = eval(condition.get("impressions", 0))
        condition.update({
            "attention": condition.get("impressions"),
            "discovery": int(condition.get("reach")),
            "destination_spend": condition.get("ai_spend_cap") * flight,
            "destination_target": condition.get("destination") * flight,
            "cost_per_action": condition.get("spend") / condition.get("action") if condition.get("action")!=0 else 1,
        })
        return {k: v for k, v in condition.items() if k in self._condition_field}
        
        
class AdSet(object):
    _condition_field = [
        "action", "desire", "interest", "awareness", "impressions", "destination_spend", "destination_target", 
        "bid_amount", "cost_per_action", "spend", "KPI", "destination_type"
    ]
    def __init__(self, campaign, adset_id):
        self.campaign = campaign
        self.adset_id = adset_id
        self.destination_type = self.campaign.destination_type
        self.service_collect = collector.AdSets(self.adset_id, charge_type=None)
        self.condition = self._create_condition(time_slice=collector.DatePreset.lifetime)
        self.condition_7d = self._create_condition(time_slice=collector.DatePreset.last_7d)
        
    def _create_condition(self, time_slice=None):
        period = (self.campaign.brief_dict.get("ai_stop_date")-self.campaign.brief_dict.get("ai_start_date")).days
        if not time_slice or time_slice==collector.DatePreset.lifetime:
            condition = self.service_collect.generate_info(date_preset = collector.DatePreset.lifetime)
            time_delta = datetime.date.today()-self.campaign.brief_dict.get("ai_start_date")
            flight = (time_delta.days / period) if (time_delta.days!=0) else (1 / period)
        elif time_slice==collector.DatePreset.last_7d:
            condition = self.service_collect.generate_info(date_preset = collector.DatePreset.last_7d)
            flight = (7 / period)
        else: raise ValueError("time_slice should be 'lifetime' or 'last_7d'.")
        
        condition['spend'] = eval(condition.get("spend", 0))
        condition['impressions'] = eval(condition.get("impressions", 0))
        condition.update({
            "KPI": self.campaign.condition.get("KPI"),
            "destination_type": self.destination_type,
            "destination_spend": self.campaign.condition.get("destination_spend"),
            "destination_target": self.campaign.condition.get("destination_target"),
            "cost_per_action": condition.get("spend") / int(condition.get("action")) if int(condition.get("action")) != 0 else 0
        })
        return {k: v for k, v in condition.items() if k in self._condition_field}
        
    def __create_fitness(self):
        self.fitness = Chromosome(self.condition)
        
    def __create_optimal_weight(self):
        self.optimal_weight = OptimalWeight(self.destination_type)


# In[ ]:


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
    def __init__(self, genetic_campaign, optimal_weight):
        super().__init__(optimal_weight)
        if genetic_campaign.destination_type in collector.BRANDING_CAMPAIGN_LIST:
            self.desire, self.interest, self.awareness, self.discovery = 0, 0, 0, 0
            self.matrix = np.array([
                self.action, self.desire, self.interest, self.awareness, self.discovery, self.spend, self.kpi
            ])

    


# In[ ]:


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
            self.m_action = (self.condition.get("destination_target")/int(self.condition.get("action"))) if int(self.condition.get("action")) != 0 else 0
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
        self.m_spend = ( self.condition.get("destination_spend")-self.condition.get("spend")) / self.condition.get("destination_spend")
        
    def __create_m_kpi(self):
        self.m_kpi = ( self.condition.get("KPI")-self.condition.get("cost_per_action") ) / self.condition.get("KPI")
    
    def __create_matrix(self):
        self.matrix = np.array([
            self.m_action, self.m_desire, self.m_interest, self.m_awareness, self.m_discovery, self.m_spend, self.m_kpi
        ])
    


# In[ ]:


if __name__ == "__main__":
    main()
    import gc
    gc.collect()
#     main(campaign_id=23843467729120098)


# In[ ]:


# !jupyter nbconvert --to script genetic_algorithm.ipynb

