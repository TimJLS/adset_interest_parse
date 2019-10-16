#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from googleads import adwords
import numpy as np
import random
import copy
import matplotlib.pyplot as plt
import math
import datetime
import pandas as pd
import gdn_datacollector as collector
import google_adwords_controller as controller
import database_controller
import adgeek_permission as permission


# In[ ]:


sizepop, vardim, MAXGEN, params = 1000, 7, 15, [0.9, 0.5, 0.5]
# sizepop, vardim, MAXGEN, params = 1000, 3, 10, [0.9, 0.5, 0.5]
ASSESSMENT_PERIOD = 14
BIDDING_INDEX = {
    'cpc': 'cpc_bid',
    'cpa': 'cpa_bid',
    'Target CPA': 'cpa_bid',    
}
TARGET_INDEX = {
    'cpc': 'clicks',
    'cpa': 'conversions',
    'Target CPA': 'cpa_bid',
}
CRITERIA_LIST = ['ADGROUP', 'URL', 'CRITERIA', 'AGE_RANGE', 'DISPLAY_KEYWORD', 'AUDIENCE', 'DISPLAY_TOPICS']

SCORE_COLUMN_INDEX = {
    'ADGROUP': ['campaign_id', 'adgroup_id', 'score'],
    'URL': ['campaign_id', 'adgroup_id', 'url_display_name', 'score'],
    'CRITERIA': ['campaign_id', 'adgroup_id', 'keyword_placement', 'keyword_id', 'score'],
    'AUDIENCE': ['campaign_id', 'adgroup_id', 'audience', 'criterion_id', 'score'],
    'AGE_RANGE': ['campaign_id', 'adgroup_id', 'age_range', 'criterion_id', 'score'],
    'DISPLAY_KEYWORD': ['campaign_id', 'adgroup_id', 'keyword', 'keyword_id', 'score'],
    'KEYWORDS': ['campaign_id', 'adgroup_id', 'keyword', 'keyword_id', 'score'],
    'DISPLAY_TOPICS': ['campaign_id', 'adgroup_id', 'topics', 'criterion_id', 'vertical_id', 'score'],
}


# In[ ]:


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
            self.population[i].calculate_fitness()
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
#         self.trace[self.t, 0] = (1 - self.best.fitness) / self.best.fitness
#         self.trace[self.t, 1] = (1 - self.avefitness) / self.avefitness
        self.trace[self.t, 0] = self.best.fitness
        self.trace[self.t, 1] = self.avefitness
        print("Generation %d: optimal function value is: %f; average function value is %f" % (
            self.t, self.trace[self.t, 0], self.trace[self.t, 1]))
        while (self.t < self.MAXGEN - 1):
            self.t += 1
            self.selection_operation()
            self.crossover_operation()
            self.mutation_operation()
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
        self.print_result()
        return self.best.chrom

    def selection_operation(self):
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

    def crossover_operation(self):
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

    def mutation_operation(self):
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

    def print_result(self):
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

    def calculate_fitness(self):
        '''
        calculate the fitness of the chromsome
        '''
        optimal_weight = OptimalWeight(self.chrom)
        self.fitness = np.dot(optimal_weight.matrix, chromosome.matrix)


# In[ ]:


class Campaign(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign_id):
        self.campaign_id = campaign_id
        self.__get_brief()
        self.destination_type = self.brief_dict.get("destination_type")
        self.collector_campaign = collector.Campaign(
            self.brief_dict.get("customer_id"), self.campaign_id, database_gdn=database_gdn)
        self.service_container = controller.AdGroupServiceContainer(self.brief_dict.get("customer_id"))
        self.controller_campaign = controller.Campaign(self.service_container, self.campaign_id)
        self.__create_condition()
        self.__get_ad_groups()
        self.__get_display_keywords()
        self.__get_audiences()
        self.__get_display_topics()
        
    def __get_display_keywords(self):
        df = database_gdn.retrieve("display_keyword_insights", campaign_id=self.campaign_id)
        display_keyword_list = list(df.groupby(['adgroup_id', 'keyword_id']).groups.keys())
        condition_list = [df.groupby(['adgroup_id', 'keyword_id']).get_group(
            (ad_group_id, keyword_id)).to_dict('records')[0] for (ad_group_id, keyword_id) in display_keyword_list]
        self.display_keywords = [ DisplayKeyword(self, condition) for condition in condition_list ]
        
    def __get_audiences(self):
        df = database_gdn.retrieve("audience_insights", campaign_id=self.campaign_id)
        audience_list = list(df.groupby(['adgroup_id', 'audience']).groups.keys())
        condition_list = [df.groupby(['adgroup_id', 'audience']).get_group(
            (ad_group_id, audience)).to_dict('records')[0] for (ad_group_id, audience) in audience_list]
        self.audiences = [ Audience(self, condition) for condition in condition_list ]
        
    def __get_display_topics(self):
        df = database_gdn.retrieve("display_topics_insights", campaign_id=self.campaign_id)
        display_topic_list = list(df.groupby(['adgroup_id', 'vertical_id']).groups.keys())
        condition_list = [df.groupby(['adgroup_id', 'vertical_id']).get_group(
            (ad_group_id, vertical_id)).to_dict('records')[0] for (ad_group_id, vertical_id) in display_topic_list]
        self.display_topics =  [ DisplayTopic(self, condition) for condition in condition_list ]
        
    def __get_ad_groups(self):
        self.controller_campaign.generate_ad_group_id_type_list()
        ad_group_id_list = self.controller_campaign.native_ad_group_id_list
        self.ad_groups = [ AdGroup(self, ad_group_id) for ad_group_id in ad_group_id_list ]
        
    def __get_brief(self):
        df_list = database_gdn.get_one_campaign(self.campaign_id).to_dict('records')
        self.brief_dict = df_list[0]
        self.brief_dict['KPI'] = self.brief_dict.get("ai_spend_cap")/self.brief_dict.get("destination")
        
    def get_weight(self):
        optimal_weight_list = database_gdn.retrieve("optimal_weight", self.campaign_id).to_dict('records')
        return optimal_weight_list[0]
        
    def __create_condition(self):
        self.condition = self.collector_campaign.get_campaign_insights(date_preset = collector.DatePreset.lifetime)
        init_list = ['impressions', 'clicks', 'all_conversions', 'view_conversions', 'conversions', ]
        action_list = ["attention", "awareness", "interest", "desire", "action", ]
        for idx, ele in enumerate(init_list):
            self.condition[action_list[idx]] = self.condition.pop(ele)
        self.condition.update(self.brief_dict)
        self.condition.update({
            "flight": (datetime.date.today()-self.brief_dict.get("ai_start_date")).days
        })
        self.condition['spend'] = int(self.condition.get("spend", 0))
        self.condition['impressions'] = int(self.condition.get("impressions", 0))
        self.condition.update({
            "attention": self.condition.get("impressions"),
            "destination_daily_spend": self.condition.get("ai_spend_cap") / self.condition.get("period"),
            "destination_daily_target":self.condition.get("destination") / self.condition.get("period"),
            "cost_per_action": self.condition.get("cost_per_target", 0),
            "spend": self.condition.get("spend") / self.condition.get("flight") if self.condition.get("flight") != 0 else 1,
            "action": self.condition.get("target") / self.condition.get("flight") if self.condition.get("flight") != 0 else 1,
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}


# In[ ]:


class AdGroup(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign, ad_group_id):
        self.campaign = campaign
        self.ad_group_id = ad_group_id
        self.destination_type = self.campaign.destination_type
        self.collector_ad_group = collector.AdGroup(
            self.campaign.brief_dict.get("customer_id"), self.campaign.campaign_id, self.ad_group_id, database_gdn=database_gdn)
        self.__create_condition()
        self.__create_optimal_weight()
    
    def __create_condition(self):
        self.condition = self.collector_ad_group.get_adgroup_insights(date_preset = collector.DatePreset.lifetime)[0]
        init_list = ['impressions', 'clicks', 'all_conversions', 'view_conversions', 'conversions', ]
        action_list = ["attention", "awareness", "interest", "desire", "action", ]
        for idx, ele in enumerate(init_list):
            self.condition[action_list[idx]] = self.condition.pop(ele)
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
        self.fitness = ObjectChromosome(self.condition)
        
    def __create_optimal_weight(self):
        self.optimal_weight = OptimalWeight(self.destination_type)


# In[ ]:


class Audience(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign, condition):
        self.campaign = campaign
        self.destination_type = campaign.destination_type
        self.condition = condition
        self.ad_group_id = condition.get('adgroup_id')
        self.criterion_id = condition.get('criterion_id')
        self.audience = condition.get('audience')
        self.__create_condition()
        self.__create_fitness()
    
    def __create_condition(self):
        init_list = ['impressions', 'clicks', 'all_conversions', 'view_conversions', 'conversions', ]
        action_list = ["attention", "awareness", "interest", "desire", "action", ]
        for idx, ele in enumerate(init_list):
            self.condition[action_list[idx]] = self.condition.pop(ele)
#         self.condition.update(self.brief_dict)
        self.condition.update({
            "KPI": self.campaign.condition.get("KPI"),
            "destination_type": self.destination_type,
            "destination_daily_spend": self.campaign.condition.get("destination_daily_spend"),
            "destination_daily_target": self.campaign.condition.get("destination_daily_target"),
            "cost_per_action": int(self.condition.get("spend")) / int(self.condition.get("action")) if int(self.condition.get("action")) != 0 else 0
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}
        
    def __create_fitness(self):
        self.fitness = ObjectChromosome(self.condition)


# In[ ]:


class DisplayKeyword(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign, condition):
        self.campaign = campaign
        self.destination_type = campaign.destination_type
        self.condition = condition
        self.ad_group_id = condition.get('adgroup_id')
        self.keyword_id = condition.get('keyword_id')
        self.keyword = condition.get('keyword')
        self.__create_condition()
        self.__create_fitness()
    
    def __create_condition(self):
        init_list = ['impressions', 'clicks', 'all_conversions', 'view_conversions', 'conversions', ]
        action_list = ["attention", "awareness", "interest", "desire", "action", ]
        for idx, ele in enumerate(init_list):
            self.condition[action_list[idx]] = self.condition.pop(ele)
#         self.condition.update(self.brief_dict)
        self.condition.update({
            "KPI": self.campaign.condition.get("KPI"),
            "destination_type": self.destination_type,
            "destination_daily_spend": self.campaign.condition.get("destination_daily_spend"),
            "destination_daily_target": self.campaign.condition.get("destination_daily_target"),
            "cost_per_action": int(self.condition.get("spend")) / int(self.condition.get("action")) if int(self.condition.get("action")) != 0 else 0
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}
        
    def __create_fitness(self):
        self.fitness = ObjectChromosome(self.condition)


# In[ ]:


class DisplayTopic(object):
    __condition_field = ["action", "desire", "interest", "awareness", "attention", "impressions", "destination_daily_spend",
                       "destination_daily_target", "cost_per_action", "spend", "daily_spend", "daily_target", "KPI", "destination_type"]
    def __init__(self, campaign, condition):
        self.campaign = campaign
        self.destination_type = campaign.destination_type
        self.condition = condition
        self.ad_group_id = condition.get('adgroup_id')
        self.criterion_id = condition.get('criterion_id')
        self.vertical_id = condition.get('vertical_id')
        self.topics = condition.get('topics')
        self.__create_condition()
        self.__create_fitness()
    
    def __create_condition(self):
        init_list = ['impressions', 'clicks', 'all_conversions', 'view_conversions', 'conversions', ]
        action_list = ["attention", "awareness", "interest", "desire", "action", ]
        for idx, ele in enumerate(init_list):
            self.condition[action_list[idx]] = self.condition.pop(ele)
#         self.condition.update(self.brief_dict)
        self.condition.update({
            "KPI": self.campaign.condition.get("KPI"),
            "destination_type": self.destination_type,
            "destination_daily_spend": self.campaign.condition.get("destination_daily_spend"),
            "destination_daily_target": self.campaign.condition.get("destination_daily_target"),
            "cost_per_action": int(self.condition.get("spend")) / int(self.condition.get("action")) if int(self.condition.get("action")) != 0 else 0
        })
        self.condition = {k: v for k, v in self.condition.items() if k in self.__condition_field}
        
    def __create_fitness(self):
        self.fitness = ObjectChromosome(self.condition)


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
    def __init__(self, campaign_id, destination_type, optimal_weight):
        super().__init__(optimal_weight)
        self.destination_type = destination_type
        if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
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
        "action", "desire", "interest", "awareness", "attention", "spend"
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
        if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
            self.condition['action'] = self.condition['awareness']
            self.condition['awareness'] = 0
        
    def __create_m_action(self):
        if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
            self.m_action = (self.condition.get("destination_daily_target")/int(self.condition.get("action"))) if int(self.condition.get("action")) != 0 else 0
        else:
            self.m_action = (int(self.condition.get("action")) / self.condition.get("desire")) if int(self.condition.get("desire")) != 0 else 0
        
    def __create_m_desire(self):
        if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
            self.m_desire = 0
        else:
            self.m_desire = (self.condition.get("desire") / self.condition.get("interest")) if self.condition.get("interest") != 0 else 0
        
    def __create_m_interest(self):
        if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
            self.m_interest = 0
        else:
            self.m_interest = (self.condition.get("interest") / self.condition.get("awareness")) if self.condition.get("awareness") != 0 else 0
        
    def __create_m_awareness(self):
        self.m_awareness = 0
#         if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
#             self.m_awareness = 0
#         else:
#             self.m_awareness = (self.condition.get("awareness") / self.condition.get("discovery")) if self.condition.get("discovery") != 0 else 0
        
    def __create_m_discovery(self):
        self.m_discovery = 0
#         if self.destination_type in database_controller.CRUDController.BRANDING_CAMPAIGN_LIST:
#             self.m_discovery = 0
#         else:
#             self.m_discovery = (self.condition.get("discovery") / self.condition.get("attention")) if self.condition.get("attention") != 0 else 0
        
    def __create_m_spend(self):
        self.m_spend = ( self.condition.get("destination_daily_spend")-self.condition.get("spend")) / self.condition.get("destination_daily_spend")
        
    def __create_m_kpi(self):
        self.m_kpi = ( self.condition.get("KPI")-self.condition.get("cost_per_action") ) / self.condition.get("KPI")
    
    def __create_matrix(self):
        self.matrix = np.array([
            self.m_action, self.m_desire, self.m_interest, self.m_awareness, self.m_discovery, self.m_spend, self.m_kpi
        ])
    


# In[ ]:


def generate_optimal_weight(campaign_id, destination_type):
    global chromosome
    print('[campaign_id]:', campaign_id )
    print('[current time]: ', datetime.datetime.now() )
    start_time = datetime.datetime.now()
    
    campaign = Campaign(campaign_id)
    chromosome = ObjectChromosome(campaign.condition)

    bound = np.tile([[0], [1]], vardim)
    ga = GeneticAlgorithm(sizepop, vardim, bound, MAXGEN, params)
    result_optimal_weight = ga.solve()
    
    optimal_campaign = CampaignOptimalWeight(campaign_id, destination_type, result_optimal_weight)
    
    score = np.dot(optimal_campaign.matrix, chromosome.matrix)
    print('==========SCORE========')
    print(score)

    score_columns=['w_action', 'w_desire', 'w_interest', 'w_awareness', 'w_discovery', 'w_spend', 'w_bid']
    df_score = pd.DataFrame(data=[optimal_campaign.matrix], columns=score_columns, index=[0])
    df_score['campaign_id'], df_score['score'] = campaign_id, score
    database_gdn.upsert("optimal_weight", df_score.to_dict('records')[0])

    assess_ad_group(campaign, optimal_campaign)
    assess_audience(campaign, optimal_campaign)
    assess_display_keywords(campaign, optimal_campaign)
    assess_display_topics(campaign, optimal_campaign)
    print('[optimal_weight]:', optimal_campaign.matrix)
    print('[operation time]: ', datetime.datetime.now()-start_time)


# In[ ]:


def assess_ad_group(campaign_object, campaign_optimal_object):
    for ad_group in campaign_object.ad_groups:
        
        chromosome_ad_group = ObjectChromosome(ad_group.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_ad_group.matrix)
        
        print('=====[adgroup_id]=====', ad_group.ad_group_id, '==========[score]', score)

        database_gdn.insert(
            "adgroup_score", 
            {'campaign_id':campaign_object.campaign_id, 'adgroup_id':ad_group.ad_group_id, 'score':score.item()}
        )


# In[ ]:


def assess_audience(campaign_object, campaign_optimal_object):
    for audience in campaign_object.audiences:
        
        chromosome_audience = ObjectChromosome(audience.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_audience.matrix)
        
        print('=====[criterion_id]=====', audience.criterion_id, '==========[score]', score)

        database_gdn.insert(
            "audience_score", 
            {'campaign_id':campaign_object.campaign_id, 'adgroup_id':audience.ad_group_id,
             'criterion_id':audience.criterion_id, 'audience':audience.audience, 'score':score.item()}
        )


# In[ ]:


def assess_display_keywords(campaign_object, campaign_optimal_object):
    for display_keyword in campaign_object.display_keywords:
        
        chromosome_display_keyword = ObjectChromosome(display_keyword.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_display_keyword.matrix)
        
        print('=====[keyword_id]=====', display_keyword.keyword_id, '==========[score]', score)

        database_gdn.insert(
            "display_keyword_score", 
            {'campaign_id':campaign_object.campaign_id, 'adgroup_id':display_keyword.ad_group_id,
             'keyword_id':display_keyword.keyword_id, 'keyword':display_keyword.keyword, 'score':score.item()}
        )


# In[ ]:


def assess_display_topics(campaign_object, campaign_optimal_object):
    for display_topic in campaign_object.display_topics:
        
        chromosome_display_topic = ObjectChromosome(display_topic.condition)
        
        score = np.dot(campaign_optimal_object.matrix, chromosome_display_topic.matrix)
        
        print('=====[vertical_id]=====', display_topic.vertical_id, '==========[score]', score)

        database_gdn.insert(
            "display_topics_score", 
            {'campaign_id':campaign_object.campaign_id, 'adgroup_id':display_topic.ad_group_id,
             'criterion_id': display_topic.criterion_id, 'vertical_id':display_topic.vertical_id, 
             'topics':display_topic.topics, 'score':score.item()}
        )


# In[ ]:


def retrive_all_criteria_insights():
    campaign_list = database_gdn.get_running_campaign().to_dict('records')
    # retrive all criteria insights
    for campaign in campaign_list:
        print('[campaign_id]: ', campaign['campaign_id'])
        customer_id = campaign['customer_id']
        campaign_id = campaign['campaign_id']
        destination_type = campaign['destination_type']
        adwords_client = permission.init_google_api(customer_id)
        camp = collector.Campaign(customer_id, campaign_id, database_gdn=database_gdn)
        for criteria in CRITERIA_LIST:
            camp.get_performance_insights( performance_type=criteria, date_preset='LAST_14_DAYS' )


# In[ ]:


def main():
    starttime = datetime.datetime.now()
    print('[start time]: ', starttime)
    global database_gdn
    db = database_controller.Database()
    database_gdn = database_controller.GDN(db)
    retrive_all_criteria_insights()
    campaign_list = database_gdn.get_branding_campaign().to_dict('records')
    print([campaign['campaign_id'] for campaign in campaign_list])
    for campaign in campaign_list:
        campaign_id = campaign.get("campaign_id")
        destination_type = campaign.get("destination_type")
        generate_optimal_weight(campaign_id, destination_type)
    
    campaign_list = database_gdn.get_performance_campaign().to_dict('records')
    for campaign in campaign_list:
        campaign_id = campaign.get("campaign_id")
        destination_type = campaign.get("destination_type")
        generate_optimal_weight(campaign_id, destination_type)
        
#     campaign_list = database_gdn.get_custom_performance_campaign().to_dict('records')
#     for campaign in campaign_list:
#         campaign_id = campaign.get("campaign_id")
#         destination_type = campaign.get("destination_type")
#         generate_optimal_weight(campaign_id, destination_type) 
        
    print('[total operation time]: ', datetime.datetime.now()-starttime)
    print('genetic algorithm finish.')
    return


# In[ ]:


if __name__ == "__main__":
    main()


# In[ ]:


# !jupyter nbconvert --to script genetic_algorithm_gdn.ipynb


# In[ ]:




