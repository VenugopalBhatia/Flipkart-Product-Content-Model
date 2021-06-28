# -*- coding: utf-8 -*-

import requests
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
from ast import literal_eval
import json
import sys
import os

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

class SpellCheck:
    # get data
    def __init__(self):
        self.tool = language_tool_python.LanguageTool('en-US')
    # query_readyprod = f""" 

    #     Select distinct a.channel_sku_id,a.sku_title,b.keyword_id,c.name,d.search_volume from entity.product_feature_mapping a 
    #     left join client_resource.client_keyword_mapping b on a.channel_sku_id = b.sku_id left join entity.keyword_master c
    #     on b.keyword_id = c.id left join (Select searchterm, avg(search_volume) as search_volume from ams.ams_seller_search where 
    #     EXTRACT(MONTH FROM input_period_start_day) = 11 and EXTRACT(Year FROM input_period_start_day)= 2020 
    #     Group By searchterm) d on lower(c.name) = d.searchterm 
    #     where a.brand='{brand}' 
    #     order by a.channel_sku_id,b.keyword_id"""
    def getData(self,df_FlipkartData):
            
        pg = postgres_conn()
        conn = pg.getConn()
        categoryList = tuple(df_FlipkartData['category'].unique())
        subCategoryList = tuple(df_FlipkartData['sub_category'].unique())
        query_readyprod = f""" 

            Select distinct b.category,b.sub_category,b.keyword,c.search_volume from 
            entity.keyword_category b left join processed_data.keyword_search_vol c
            on b.keyword = c.keyword  where 
            c.month = 3 and c.year = 2021 and b.category in {categoryList} and c.sub_category in {subCategoryList}
            """

        

        df_readyprod = pd.read_sql_query(query_readyprod, conn[1])
        # df_readyprod.fillna("",inplace= True)
        df_FlipkartData = df_FlipkartData.merge(df_readyprod,how = 'left',on = ['category','sub_category'])
        df_FlipkartData.fillna("",inplace= True)
        df_FlipkartData.to_excel('FlipkartData_temp.xlsx')
        return df_FlipkartData

    ##################################################################


    # Word Count ########################################################################

    def title_word_count(self,df,col):
        return df[col].str.len()

    

    # Word Duplicate Count ########################################################################

    def title_word_duplicate_count(self,df,title,duplicateTitleCount):
       
        df[duplicateTitleCount] = np.nan
        for i in range(len(df[title])):
           df.at[i,duplicateTitleCount] = len(df.at[i,title].split()) - len(set(df.at[i,title].split()))
        print(duplicateTitleCount+ " column added to df")
        

    

    # Spell Checker ########################################################################

    

        
    def title_spell_check_df(self,df,title,errorList,ignore_Keywords):
        print("Inside title spell check df")
        df_uniqueProductTitle = df[title].drop_duplicates()
        df[errorList] = '[]'
        df['spelling_errors_flags'] = '[]'
        df[errorList] = df[errorList].astype(object)
        
        for ith_title in df_uniqueProductTitle:
       
            list_error = title_spell_check(ith_title,self.tool)
            flag_spellings = {}
            try:
                for i in range(len(list_error)):
                    if list_error[i].replace("'","").strip(" ").lower() in ignore_Keywords:
                        flag_spellings[list_error[i].replace("'","").strip(" ")] = 0
                    elif list_error[i].replace("'","").strip(" ")!='' :
                        flag_spellings[list_error[i].replace("'","").strip(" ")] = 1
                
                df.loc[df[title] == ith_title,'spelling_errors_flags'] = str(flag_spellings)

                df.loc[df[title] == ith_title,errorList] = str(list_error)
                
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("***** error in title spelling check df:",exc_type, exc_obj,fname,exc_tb.tb_lineno)
        try:
            df.loc[:,errorList] = df.loc[:,errorList].apply(lambda x: literal_eval(x))
            df.loc[:,'spelling_errors_flags'] = df.loc[:,'spelling_errors_flags'].apply(lambda x: literal_eval(x))
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("***** error in title spelling check df:",exc_type, exc_obj,fname,exc_tb.tb_lineno)
        
    
        
                 

   #  check for description formatting#############

    


    def fuzzy_extract_df(self,df,sku_title,keyword):
        try:
            print("Inside fuzzy extract df")
            df["Match_Keyword"] = ""
            df["Match_Score"] = 0
            df["Weighted"] = 0
            for ind in df.index:
                title = df[sku_title][ind]
                key = df[keyword][ind]
                try:   
                    x,y = fuzzy_extract(key, title)
                    df.at[ind,"Match_Keyword"] = x
                    df.at[ind,"Match_Score"] = y     
                except:
                    df.at[ind,"Match_Keyword"] = ""
                    df.at[ind,"Match_Score"] = 0


            df["Weighted"] = df.apply(lambda row:(row['search_volume']*row['Match_Score']),axis=1)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("***** error in title spelling check df:",exc_type, exc_obj,fname,exc_tb.tb_lineno)


    def getWeightedMatchScore(self,df,asin,title,search_volume,weighted,weighted_first_eighty):
        df_agg = df.groupby([asin,title]).agg(
        Sum_search = pd.NamedAgg(column = search_volume, aggfunc = sum),
        Sum_weighted = pd.NamedAgg(column = weighted, aggfunc = sum),
        Sum_weighted_first_eighty = pd.NamedAgg(column = weighted_first_eighty, aggfunc = sum)
        )
        # df_agg["Match_Score_Weighted"] = 0
        try:
            df_agg["Match_Score_Weighted"] = df_agg.apply(lambda row:(row['Sum_weighted']/row['Sum_search']),axis=1)
        except:
            df_agg["Match_Score_Weighted"] = 0
        try:
            df_agg["Match_Score_Weighted_FirstEighty"] = df_agg.apply(lambda row:(row['Sum_weighted_first_eighty']/row['Sum_search']),axis=1)
        except:
            df_agg["Match_Score_Weighted_FirstEighty"] = 0
        return df_agg
    

    def fuzzy_extract_df_firstEightyChars(self,df,sku_title,keyword):
        try:
            df["Match_Keyword_firstEighty"] = ""
            df["Match_Score_firstEighty"] = 0
            for ind in df.index:
                title = df[sku_title][ind]
                print(title)
                if((title is not None) and (len(title)>80)):
                    title = title[0:80]
                key = df[keyword][ind]
                try:
                    x,y = fuzzy_extract(key, title)
                    df.at[ind,"Match_Keyword_firstEighty"] = x
                    df.at[ind,"Match_Score_firstEighty"] = y
                    
                except:
                    df.at[ind,"Match_Keyword_firstEighty"] = ""
                    df.at[ind,"Match_Score_firstEighty"] = 0

            df["Weighted_FirstEighty"] = df.apply(lambda row:(row['search_volume']*row['Match_Score_firstEighty']),axis=1)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("***** error in title spelling check df:",exc_type, exc_obj,fname,exc_tb.tb_lineno)

#################################################################################################################

def title_spell_check(title,tool):
    list_error = []
    if(title != None):
        matches = tool.check(title)
        for i in matches:
            if i.ruleIssueType == 'misspelling':
                list_error.append(title[i.offset:i.offset+i.errorLength])
    return list_error 


def fuzzy_extract(keyword,title):
    keyword = keyword.lower()
    title = title.lower()
    kw = keyword.split(" ")
    
    count = len(kw)
    
    s2 = 0
    kwd2 = ""
    
    for k in kw:
        s1 = 0
        kwd1 = ""
        for match in find_near_matches(k, title, max_l_dist=1):
            match = match.matched
            index = fuzz.WRatio(match, k)
            if index>70 and index>s1:
                kwd1 = match
                s1 = index
                
        kwd2 = kwd2 + " " + kwd1
        if s2 == 0:
            s2 = s1
        else:
            s2 = (s2+s1)
    return (kwd2,s2/count)

