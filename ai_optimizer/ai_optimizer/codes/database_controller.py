#!/usr/bin/env python
# coding: utf-8

# In[11]:


import mysql.connector
import pandas as pd
import datetime
# from pandas.io import sql
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData
from sqlalchemy import sql
from sqlalchemy.dialects import mysql
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb


# In[12]:


class Database(object):
    host = "aws-prod-ai-private.adgeek.cc"
    user = "app"
    password = "adgeek1234"


# In[19]:


class DevDatabase(Database):
    host = "aws-dev-ai-private.adgeek.cc"


# In[14]:


class CRUDController(object):
    dt = datetime.date.today()
    metrics_converter = {
        'facebook': {
            'account_id': sql.column("account_id"),
            'campaign_id': sql.column("campaign_id"),
            'adset_id': sql.column("adset_id"),
            'table_init_bid': "adset_initial_bid",
            'table_insights': "adset_metrics",
            'score': "adset_score",
            'optimal_weight': "campaign_optimal_weight",
            'campaign_target_suggestion': "campaign_target_suggestion",
            'facebook_campaign_currency': "facebook_campaign_currency",
            'campaign_pixel_id': "campaign_pixel_id",
            'campaign_target': "campaign_target",
            'custom_conversion': "facebook_custom_conversion",
            'facebook_adset_optimization_goal': "facebook_adset_optimization_goal",
            'account_target_suggestion': "account_target_suggestion",
        },
        'gdn': {
            'campaign_id': sql.column("campaign_id"),
            'adset_id': sql.column("adgroup_id"),
            'table_init_bid': "adgroup_initial_bid",
            'table_insights': "adgroup_insights",
            'score': "adgroup_score",
            'optimal_weight': "optimal_weight",
        },
        'gsn': {
            'campaign_id': sql.column("campaign_id"),
            'adset_id': sql.column("adgroup_id"),
            'table_init_bid': "adgroup_initial_bid",
            'table_insights': "keywords_insights",
            'score': "adgroup_score",
            'optimal_weight': "optimal_weight",
        }
    }
    BRANDING_CAMPAIGN_LIST = [
        'THRUPLAY', 'LINK_CLICKS', 'ALL_CLICKS', 'VIDEO_VIEWS', 'REACH', 'POST_ENGAGEMENT', 'PAGE_LIKES', 'LANDING_PAGE_VIEW']
    PERFORMANCE_CAMPAIGN_LIST = [
        'PURCHASE', 'MESSAGES', 'SEARCH', 'INITIATE_CHECKOUT', 'LEAD_WEBSITE', 'PURCHASES', 'ADD_TO_WISHLIST', 'VIEW_CONTENT', 'ADD_PAYMENT_INFO', 'COMPLETE_REGISTRATION', 'CONVERSIONS', 'LEAD_GENERATION', 'ADD_TO_CART']
    CUSTOM_CAMPAIGN_LIST = [
        'CUSTOM', 'CONVERSIONS',
    ]
    def __init__(self, database):
        self.database = database
        
    def dispose(self):
        self.engine.dispose()
    
    def get_one_campaign(self, campaign_id):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        tbl.c.campaign_id == campaign_id,
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
    
    def get_running_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
    
    def get_performance_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        tbl.c.destination_type.in_(self.PERFORMANCE_CAMPAIGN_LIST + self.CUSTOM_CAMPAIGN_LIST),
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
        
    def get_standard_performance_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        tbl.c.destination_type.in_(self.PERFORMANCE_CAMPAIGN_LIST),
                        tbl.c.custom_conversion_id == None,
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
        
    def get_custom_performance_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        tbl.c.destination_type.in_(self.CUSTOM_CAMPAIGN_LIST),
                        tbl.c.custom_conversion_id != None,
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
        
    def get_branding_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_( 
                        tbl.c.destination_type.in_(self.BRANDING_CAMPAIGN_LIST),
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
    
    def get_not_opted_campaign(self,):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            return pd.read_sql(
                sql.select(['*'], from_obj=tbl).where(
                    sql.and_(
                        sql.or_(
                            sql.func.date(tbl.c.optimized_date) != '{:%Y/%m/%d}'.format(self.dt), 
                            sql.func.date(tbl.c.optimized_date) == None, 
                        ), 
                        sql.func.date(tbl.c.ai_stop_date) >= '{:%Y/%m/%d}'.format(self.dt),
                        sql.func.date(tbl.c.ai_start_date) <= '{:%Y/%m/%d}'.format(self.dt),
                        tbl.c.ai_status == 'active',
                    )
                ), con=self.conn
            )
        
    def get_brief(self, campaign_id):
        with self.engine.connect() as self.conn:
            tbl = Table("campaign_target", self.metadata, autoload=True)
            df = pd.read_sql(
                sql.select([
                    tbl.c.ai_spend_cap, tbl.c.ai_start_date, tbl.c.ai_stop_date, tbl.c.destination_type, tbl.c.custom_conversion_id], from_obj=tbl).where(
                    sql.and_(
                        tbl.c.ai_status == 'active',
                        tbl.c.campaign_id == campaign_id,
                    )
                ), con=self.conn,
            )
            return df.to_dict('rocords')[0] if not df.empty else {}
        
    def get_init_bid(self, adset_id):
#         adset_id = self.metrics_converter[self.media]['adset_id']
        with self.engine.connect() as self.conn:
            tbl = Table(self.metrics_converter[self.media]['table_init_bid'], self.metadata, autoload=True)
            query = sql.select([tbl.c.bid_amount], from_obj=tbl).where(
                    sql.and_(
                        self.metrics_converter[self.media]['adset_id'] == adset_id,
                    )
                )
            results = self.conn.execute( query ).fetchall()
            for (result, ) in results:
                return result
    
    def get_last_bid(self, adset_id):
#         adset_id = self.metrics_converter[self.media]['adset_id']
        with self.engine.connect() as self.conn:
            tbl = Table(self.metrics_converter[self.media]['table_insights'], self.metadata, autoload=True)
            query = sql.select([tbl.c.bid_amount], from_obj=tbl).where(
                    sql.and_(
                        self.metrics_converter[self.media]['adset_id'] == adset_id,
                    )
                ).order_by(sql.desc(tbl.c.request_time)).limit(1)
            results = self.conn.execute( query ).fetchall()
            for (result, ) in results:
                return result

    def retrieve(self, table_name, campaign_id=None, adset_id=None, account_id=None, by_request_time=True):
        table_name = self.metrics_converter[self.media][table_name]
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            if campaign_id:
                if by_request_time:
                    stmt =  sql.select(['*'], from_obj=tbl).where(sql.and_(
                        self.metrics_converter[self.media]['campaign_id'] == campaign_id,
                        sql.func.date(tbl.c.request_time) == '{:%Y/%m/%d}'.format(self.dt),
                    ))
                    return pd.read_sql( stmt, con=self.conn,)
                else:
                    stmt = sql.select(['*'], from_obj=tbl).where(sql.and_(
                        self.metrics_converter[self.media]['campaign_id'] == campaign_id,
                    ))
                    return pd.read_sql( stmt, con=self.conn,)
            elif adset_id:
                stmt = sql.select(['*'], from_obj=tbl).where( sql.and_(
                    self.metrics_converter[self.media]['adset_id'] == adset_id,
                ))
                return pd.read_sql( stmt, con=self.conn,)
            elif account_id:
                stmt = sql.select(['*'], from_obj=tbl).where( sql.and_(
                    self.metrics_converter[self.media]['account_id'] == account_id,
                ))
                return pd.read_sql( stmt, con=self.conn,)
            else:
                self.engine.dispose()
    def retrieve_all(self, table_name, ):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            stmt =  sql.select(['*'], from_obj=tbl)
            return pd.read_sql( stmt, con=self.conn,)
        
    def insert(self, table_name, values_dict):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            ins = mysql.insert(tbl).values( **values_dict )
            self.conn.execute( ins, )
            
    def upsert(self, table_name, values_dict):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            ins = mysql.insert(tbl).values( **values_dict ).on_duplicate_key_update( ** values_dict  )
            self.conn.execute( ins, )
            
    def upsert_nothing(self, table_name, values_dict):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            ins = mysql.insert(tbl).values( **values_dict ).on_duplicate_key_update()
            self.conn.execute( ins, )
            
    def insert_ignore(self, table_name, values_dict):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            ins = mysql.insert(tbl).values( **values_dict ).prefix_with('IGNORE')#.where(tbl.c.campaign_id==111)
            self.conn.execute( ins, )
            
    def update(self, table_name, values_dict, campaign_id=None, adset_id=None, audience_id=None):
        with self.engine.connect() as self.conn:
            tbl = Table(table_name, self.metadata, autoload=True)
            if campaign_id:
                stmt = sql.update(tbl).where( tbl.c.campaign_id==campaign_id ).values( **values_dict )
            elif adset_id:
                stmt = sql.update(tbl).where( tbl.c.adset_id==adset_id ).values( **values_dict )
            elif audience_id:
                stmt = sql.update(tbl).where( tbl.c.audience_id==audience_id ).values( **values_dict )
            else:
                return self.engine.dispose()
            self.conn.execute( stmt, )
            
    def update_init_bid(self, campaign_id, update_ratio=1.1):
        with self.engine.connect() as self.conn:
            tbl = Table(self.metrics_converter[self.media]['table_init_bid'], self.metadata, autoload=True)
            query = sql.select([tbl.c.adset_id, tbl.c.bid_amount], from_obj=tbl).where(
                    sql.and_(
                        self.metrics_converter[self.media]['campaign_id'] == campaign_id,
                    )
                )
            results = self.conn.execute( query ).fetchall()
            for (adset_id, bid_amount) in results:
                bid_amount = bid_amount * update_ratio
                self.update(self.metrics_converter[self.media]['table_init_bid'], {'bid_amount': bid_amount}, adset_id=adset_id)
#             return results


# In[15]:


class FB(CRUDController):
    __database = 'dev_facebook_test'
    def __init__(self, database):
        super().__init__(database)
        self.engine = create_engine(
            'mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database
            ),
            poolclass=NullPool,
        )
        print('mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database))
        self.metadata = MetaData(bind=self.engine)
        self.table_init_bid = 'adset_initial_bid'
        self.media = 'facebook'


# In[5]:


class GDN(CRUDController):
    __database = 'dev_gdn'
    def __init__(self, database):
        super().__init__(database)
        self.engine = create_engine(
            'mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database
            ),
            poolclass=NullPool,
        )
        print('mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database
        ))
        self.metadata = MetaData(bind=self.engine)
        self.table_init_bid = 'adgroup_initial_bid'
        self.media = 'gdn'


# In[6]:


class GSN(CRUDController):
    __database = 'dev_gsn'
    def __init__(self, database):
        super().__init__(database)
        self.engine = create_engine(
            'mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database
            ),
            poolclass=NullPool,
        )
        print('mysql://{user}:{password}@{host}/{database}'.format(
                user=self.database.user, password=self.database.password, host=self.database.host, database=self.__database
        ))


# In[ ]:


# !jupyter nbconvert --to script database_controller.ipynb


# In[ ]:




