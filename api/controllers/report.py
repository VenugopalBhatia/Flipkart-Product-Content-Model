import utils
import pandas as pd
import importlib
importlib.reload(utils)
import imageTextVision
import consolidatedFile

importlib.reload(consolidatedFile)
import scraper
importlib.reload(imageTextVision)
import scoringFile
importlib.reload(scoringFile)
import os
import sys

from pathlib import Path
from models import connect

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import requests
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC

# Create dataframe with product title,category,subcategory,keyword and search volume


def genReport(df_FlipkartCategoryData,ignore_keywords):

    asinList = list(df_FlipkartCategoryData['channel_sku_id'].unique())
    driver = webdriver.Chrome(executable_path = r'C:\Users\Administrator\Downloads\chromedriver.exe')
    contentScoresDb = connect('contentScoresDb_Flipkart')
    locator = 'div>div>div>h1'
    timeout = 10
    asin_start_index = 0 #change to asin_current_index to resume loop from the point where it was paused
    # asin_start_index = asin_current_index #
    # asin_details = {} ## comment this line and update asin_details with asin_page before restarting loop
    asin_page,asin_current_index = scraper.getData(asinList,asin_start_index,timeout,locator,driver)
    # asin_details.update(asin_page)
    print("asin details", asin_page)
    df_FlipkartData = pd.DataFrame.from_dict(asin_page,orient = 'index')
    driver.close()
    df_FlipkartData.reset_index(level = 0,inplace=True)
    df_FlipkartData.rename(columns = {'index':'channel_sku_id'},inplace=True)
    df_FlipkartData['asin'] = df_FlipkartData['channel_sku_id']
    df_FlipkartData.to_excel('FlipkartData.xlsx')
    # df_FlipkartData = pd.read_excel('FlipkartData.xlsx',engine='openpyxl')
    df_FlipkartData.rename(columns = {'images':'catalog_images'},inplace = True)
    date_ = df_FlipkartData['timestamp'][0]
    date_ = date_.split(" ")[0]
     # Details of aplus images from google vision
    # aplusImageDetailsPath = 'AplusImages' + str(date_) + '.xlsx'
    # details for product title and description
    getScoringDataPath = 'data/ProductContentScoring' + str(date_) + '.xlsx'
    # Links to catalog images scraped from amazon
    getCatalogImageLinksPath = 'data/CatalogImageLinks' + str(date_) + '.xlsx'
    # Final file with scores for content
    asinContentScoresPath = 'data/ContentScores' + str(date_) + '.xlsx'
    # File to read scoring parameters
    scoringParamsPath = 'data/ScoringParameters.xlsx'
    # Details of catalog images from google vision
    catalogImageDetailsPath = 'data/CatalogImages' + str(date_) + '.xlsx'

    # df_readyprod = imageTextVision.getAplusImageData(df_FlipkartImagesData)
    # df_ready = imageTextVision.getImagesDetails(df_readyprod,aplusImageDetailsPath,'aplus')
    # df_ready['week'] = date_
    # try:
    #     col_aplusImageDetails = contentScoresDb.createConnection('aplusImageDetails')
    #     col_aplusImageDetails.insert_many(df_ready.to_dict('records'),ordered = False)
    # except:
    #     print("****** mongo err",sys.exc_info())
    #     pass

    try:
        df_FlipkartData = df_FlipkartData.merge(df_FlipkartCategoryData,how = 'left',on = 'channel_sku_id')
        consolidatedFile.getScoringData(df_FlipkartData,ignore_keywords,getScoringDataPath)
        # catalogImages.getCatalogImages(asinListRaw,getCatalogImageLinksPath)
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("****** get data from database or scraping err details:",exc_type, exc_obj,fname,exc_tb.tb_lineno)

        pass

    writer = pd.ExcelWriter(asinContentScoresPath,engine = 'xlsxwriter')
    # try:
    #     df_aplusImageScore = scoringFile.ImageScoring(scoringParamsPath,aplusImageDetailsPath,'aplusContentScoring','aplus')
    #     df_aplusImageScore['week'] = date_
    #     df_aplusImageScore.to_excel(writer,'AplusImageScores')
    # except:
    #     print("****** pandas err",sys.exc_info())
    #     pass

    # try:
    #     col_aplusImageScore = contentScoresDb.createConnection('aplusImageScores')
    #     col_aplusImageScore.insert_many(df_aplusImageScore.to_dict('records'),ordered = False)
    # except:
    #     print("****** mongo err",sys.exc_info())
    #     pass
    
    try:
        df_FlipkartImagesData = df_FlipkartData[['channel_sku_id','catalog_images']].copy()
        df_FlipkartImagesData = df_FlipkartImagesData.explode('catalog_images',ignore_index=True)
        df_FlipkartImagesData.dropna(inplace= True)

        df_readyProdCatalog = df_FlipkartImagesData
        df_readyCatalog = imageTextVision.getImagesDetails(df_readyProdCatalog,catalogImageDetailsPath,'catalog')
    except:
        print("****** pandas err",sys.exc_info())
        pass
    try:
        col_catalogImageDetails = contentScoresDb.createConnection('catalogImageDetails')
        col_catalogImageDetails.insert_many(df_readyCatalog.to_dict('records'),ordered = False)
    except:
        print("****** mongo err",sys.exc_info())
        pass

    try:
        df_catalogImageScore = scoringFile.ImageScoring(scoringParamsPath,catalogImageDetailsPath,'imgScoring','catalog')
        df_catalogImageScore.to_excel(writer,'CatalogImageScores')
    except:
        print("****** pandas err",sys.exc_info())
        pass

    try:
        col_catalogImageScore = contentScoresDb.createConnection('catalogImageScores')
        col_catalogImageScore.insert_many(df_catalogImageScore.to_dict('records'),ordered = False)
    except:
        print("****** mongo err",sys.exc_info())
        pass

    try:    
        df_productTitle,df_productTitleAgg = scoringFile.ProductTitleScoring(scoringParamsPath,getScoringDataPath,'productTitleScoring','ProductTitle')
        df_productTitle['week'] = date_
        df_productTitle.to_excel(writer,sheet_name='ProductTitleScoring')
        df_productTitleAgg['week'] = date_
        df_productTitleAgg.to_excel(writer,'ProductTitleASINScoring')
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("****** pandas err",exc_type, exc_obj,fname,exc_tb.tb_lineno)
        pass

    try:
        col_productTitle = contentScoresDb.createConnection('productTitleScores')
        col_productTitle.insert_many(df_productTitle.to_dict('records'),ordered = False)
    except:
        print("****** mongo err",sys.exc_info())
        pass

    try:
        col_ProductTitleAgg = contentScoresDb.createConnection('productTitleASINScores')
        col_ProductTitleAgg.insert_many(df_productTitleAgg.to_dict('records'),ordered = False)
    except:
        print("****** mongo err",sys.exc_info())
        pass

    # try:
    
    #     df_productDescription = scoringFile.ProductFeatureCheck(scoringParamsPath,getScoringDataPath,'productDescriptionScoring','FeatureCheck')
    #     df_productDescription['week'] = date_
    #     df_productDescription.to_excel(writer,"ProductDescriptionScore")
    # except:
    #     exc_type, exc_obj, exc_tb = sys.exc_info()
    #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #     print("****** pandas err",exc_type, exc_obj,fname,exc_tb.tb_lineno)
    #     pass


    # try:
    #     col_productDescription = contentScoresDb.createConnection('productDescriptionScores')
    #     col_productDescription.insert_many(df_productDescription.to_dict('records'),ordered = False)
    # except:
    #     print("****** mongo err",sys.exc_info())
    #     pass

    try:
        df_mainPage = df_productTitleAgg
        # df_mainPage = df_productTitleAgg[['asin','avgProductTitleScore']].merge(df_productDescription[['asin','averageProductDescriptionScore']],on = 'asin',how = 'left')
        # df_mainPage = df_mainPage.merge(df_aplusImageScore[['channel_sku_id','avgaplusScore']],left_on = 'asin',right_on = 'channel_sku_id',how = 'left')
        df_mainPage = df_mainPage.merge(df_catalogImageScore[['channel_sku_id','avgcatalogScore']],left_on = 'asin',right_on = 'channel_sku_id',how = 'left')
        df_mainPage['week'] = date_
        df_mainPage.to_excel(writer,'ASINScores')
    
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print("****** pandas err",exc_type, exc_obj,fname,exc_tb.tb_lineno)
        writer.save()
        return {}
        

    try:
        col_mainPage = contentScoresDb.createConnection('ASINScores')
        col_mainPage.insert_many(df_mainPage.to_dict('records'),ordered = False)
    except:
        print("****** mongo err",sys.exc_info())
        pass

    writer.save()
    return df_mainPage.to_json(orient = 'records')


# Retrieve asins that are not present in database

def getAsinsNotInDb(ASINList,field,dbName,collectionName):
    contentScoresDb = connect(dbName)
    contentScoresCollection = contentScoresDb.createConnection(collectionName)

    data = contentScoresCollection.find( { field: { "$in" : ASINList } } )

    data = list(data)

    ASINSfound = [x[field] for x in data]

    
    ASINSNotFound = set(ASINList) - set(ASINSfound)

    ASINSNotFound = list(ASINSNotFound)

    return ASINSfound,ASINSNotFound,data




# asinList = ['B012ZAGA0G',
#  'B01GZSQJPA',
#  'B078GVP2GG',
#  'B00FMNH882',
#  'B085GQ6BTF',
#  'B008ETDJUM',
#  'B009UORF2I',
#  'B07GL1976K',
#  'B07T8BS1XY',
#  'B08GZFQD8N',
#  'B08B3WNG7Y',
#  'B076QB259Q',
#  'B010LE4R94',
#  'B0828YXFVS',
#  'B00HQZG41Q',
#  'B07XDLP8SJ',
#  'B07XBJ5Z9S',
#  'B009UOREII']
# asinList = ['B00NKG8UZ8','B01M8O9X0N',
#  'B01MR6KOM0',
#  'B08B64HRW2',
#  'B078GRW354',
#  'ABCDEFGH']

# asinFound,asinNotFound = getAsinsNotInDb(asinList,'channel_sku_id','contentScoresDb','ASINScores')

# print("****",asinFound,"****** found ******",asinNotFound,"***** not found *****")

# ignore_keywords = ['viva','Norelco','Aquatouch']

# scores = genReport(asinList,ignore_keywords)


