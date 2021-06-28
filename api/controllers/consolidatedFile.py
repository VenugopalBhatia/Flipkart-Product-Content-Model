import spellCheck 
import importlib
importlib.reload(spellCheck)
import pandas as pd
import re
import nltk
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import numpy as np
from fuzzysearch import find_near_matches
import descriptionCheck
importlib.reload(descriptionCheck)
import language_tool_python
import json


def getScoringData(df_FlipkartData,ignore_Keywords,ScoringFile):
    print("Inside get Scoring Data")
    writer = pd.ExcelWriter(ScoringFile, engine='xlsxwriter')
    spelling = spellCheck.SpellCheck()
    desc = descriptionCheck.DescriptionCheck()
    # methods for spelling and description
    df_spellCheckData = getSpellCheck(df_FlipkartData,ignore_Keywords,spelling)
    df_spellCheckData.to_excel(writer,"ProductTitle")
    df_spellCheckData_agg = spelling.getWeightedMatchScore(df_spellCheckData,'asin','product title','search_volume','Weighted','Weighted_FirstEighty')
    df_spellCheckData_agg.to_excel(writer,sheet_name = "Spell_Check_Data_Agg")
    # df_descriptionCheckData = getDescriptionCheck(asinList,desc,date_)
    # df_descriptionCheckData.to_excel(writer,sheet_name = "Description_Check_Data")
    # df_asin_CombinedFeatures = df_descriptionCheckData[['asin','Combined Features']]
    # df_asins_sku = df_spellCheckData[['asin','title']]
    # df_asin_CombinedFeatures.drop_duplicates(inplace=True)
    # df_asins_sku.drop_duplicates(inplace=True)
    # df_featureCheck = getFeatureCheck(df_asin_CombinedFeatures,df_asins_sku)
    # df_featureCheck.to_excel(writer,sheet_name = "FeatureCheck")
    writer.save()

def getSpellCheck(df_FlipkartData,ignore_Keywords,spelling):
    
    df_spellCheckData = spelling.getData(df_FlipkartData)
    spelling.fuzzy_extract_df(df_spellCheckData,'product title','keyword')
    spelling.fuzzy_extract_df_firstEightyChars(df_spellCheckData,'product title','keyword')
    spelling.title_spell_check_df(df_spellCheckData,'product title','errors',ignore_Keywords)
    df_spellCheckData['title word count'] = spelling.title_word_count(df_spellCheckData,"product title")
    spelling.title_word_duplicate_count(df_spellCheckData,'product title','duplicate word count')

    df_spellCheckWeightedMatchScore = spelling.getWeightedMatchScore(df_spellCheckData,'asin','product title','search_volume','Weighted','Weighted_FirstEighty')
    df_spellCheckData_updated = df_spellCheckData.merge(df_spellCheckWeightedMatchScore,how = 'left',on = ['asin','product title'])
    return df_spellCheckData_updated

def getDescriptionCheck(asinList,desc,date_):
    
    df_descriptionCheckData = desc.getdata(asinList,date_)
    desc.getnumfeatures(df_descriptionCheckData)
    desc.getnumheaders(df_descriptionCheckData)
    df_descriptionCheckData['Combined Features'] = df_descriptionCheckData['feature_1'].map(str) + " " + df_descriptionCheckData['feature_2'].map(str) + " " + df_descriptionCheckData['feature_3'].map(str) + " " + df_descriptionCheckData['feature_4'].map(str) + " " + df_descriptionCheckData['feature_5'].map(str) + " " +  df_descriptionCheckData['feature_6'].map(str) + " " + df_descriptionCheckData['feature_7'].map(str) + " " +  df_descriptionCheckData['feature_8'].map(str)
    return df_descriptionCheckData

def getFeatureCheck(df_asin_CombinedFeatures,df_asins_sku):
    df_featureCheck = df_asins_sku.merge(df_asin_CombinedFeatures,left_on = 'asin',right_on = 'asin')
    df_featureCheck['product title'] = df_featureCheck['product title'].str.replace("-",'').str.replace("/",'').str.replace('(','').str.replace(')','').str.replace("[a-zA-Z]{2}[0-9]{1,10}",'').str.replace("[A-Z]{2}\s[0-9]{1,10}",'').str.replace("\s[A-Za-z]{1}\s",'').str.replace("\s[0-9]{1,2}\s",'').str.replace("[^A-Za-z0-9\s]",'')
    df_featureCheck['featureStr'] = df_featureCheck['product title'].str.lower()
    df_featureCheck['featureList'] = df_featureCheck['featureStr'].str.strip().str.split(" ")
    for  ind in df_featureCheck.index:
        try:
            # print(df_featureCheck['featureList'][ind])
            df_featureCheck['featureList'][ind].remove("")
            # print(df_featureCheck['featureList'][ind])
        except:
            pass
    df_featureCheck['featureTokens'] = ""
    for ind in df_featureCheck.index:
        df_featureCheck['featureTokens'][ind] = []
        title = df_featureCheck['Combined Features'][ind]
        features = df_featureCheck['featureList'][ind]
        for i in range(len(features)):

            try:
                # print({features[i]:list(spellCheck.fuzzy_extract(features[i],title))})
                df_featureCheck['featureTokens'][ind].append(list(spellCheck.fuzzy_extract(features[i],title)))
            except:
                pass
    df_featureCheck['featureCount'] = 0
    for ind in df_featureCheck.index:
        count = 0
        for i in df_featureCheck['featureTokens'][ind]:
            if(i[1]!=0):
                count+=1
        df_featureCheck.at[ind,'featureCount'] = count
    df_featureCheck['totalFeatures'] = df_featureCheck['featureStr'].str.split().apply(len)
    return df_featureCheck