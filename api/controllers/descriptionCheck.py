# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt
import language_tool_python
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from fuzzysearch import find_near_matches
import statistics 

class postgres_conn:
    def getConn(self):
        try:
            connection = psycopg2.connect(user='postgres',
                                          password='postgres_007',
                                          host="1.pgsql.db.1digitalstack.com",
                                          port='5432',
                                          database='postgres')

            cursor = connection.cursor()
            # Print PostgreSQL Connection properties
            print(connection.get_dsn_parameters(), "\n")

            # Print PostgreSQL version
            return cursor, connection

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            return error, error

    def close_connection(self, cursor, connection):
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            

class DescriptionCheck:

    def getdata(self,asinList,date_):
        pg = postgres_conn()
        conn = pg.getConn()
        query_readyprod = f"""
        Select distinct a.brand, a.channel_sku_id,b.* from ready.ready_product_details b left join entity.product_feature_mapping a on a.channel_sku_id =
         b.asin where b.asin in {asinList} and DATE(b.crawl_time) = '{date_}' """
        
        df_readyprod = pd.read_sql_query(query_readyprod, conn[1])
        df_features = df_readyprod[['brand','asin', 'feature_1', 'feature_2', 'feature_3', 'feature_4',
       'feature_5', 'feature_6', 'feature_7', 'feature_8', 'fba',
       'extra_feature_1', 'extra_feature_2', 'extra_feature_3', 'aplus_text', 'aplus_images',
        'aplus_present', 'description',  'cat_lev_one', 'cat_lev_two', 'cat_lev_three',
       'cat_lev_four']]
        df_features.drop_duplicates(subset = ['asin'],inplace=True)
        return df_features

    def getnumfeatures(self,df):
        df["Number of Features"] = df.iloc[:,2:10].count(axis = 1)
    
    def getnumheaders(self,df):
        df["Number of Headers"] = df.iloc[:,2:10].apply(lambda x: x.str.contains(":")).sum(axis = 1)





#############################################################################

# for ind in df_readyprod.index:
#     count = 0
#     count1 = 0
#     for i in range(1,8):
#         col_name = "feature_"+str(i)
#         feature_val = df_readyprod[col_name][ind]
        
#         if feature_val == None : 
#             continue
#         elif len(feature_val)!=0 :
#             count = count+1
#             if ":" in feature_val:
#                 count1 = count1+1
#     Number_Of_Features.append(count)
#     Headers.append(count1)
    
# df_readyprod["Number Of features"] = Number_Of_Features
# df_readyprod["Number Of Headers"] = Headers
        
       

        