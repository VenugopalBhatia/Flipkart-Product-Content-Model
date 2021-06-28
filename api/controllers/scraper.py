import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
import requests
from selenium.webdriver.support.ui import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC
from tqdm.notebook import tqdm
import time
import sys
import datetime
import time



def getCatalogContent(driver):
    
    imgs = driver.find_elements_by_css_selector('div>ul>li>div>div[style*=background-image]')
    numVids = len(driver.find_elements_by_css_selector('div>ul>li>div>div:nth-child(3)'))
    imgList = []
    for i in imgs:
        url_ = i.value_of_css_property('background-image').strip('url("")').replace('128/','416/',2)
        imgList.append(url_)
    return imgList,numVids

def getTechnicalDetails(driver):
    details_fs = driver.find_elements_by_css_selector('table')
    df_prodTabularDetails = pd.DataFrame()
    productDetails = {}
    for tl in details_fs:
        table_content = tl.get_attribute('innerHTML')
        # pd.read_html(table_content)
        df = pd.read_html('<table>' + table_content + '</table>')[0]
        df_prodTabularDetails = df_prodTabularDetails.append(df,ignore_index=True)
    productDetails = df_prodTabularDetails.set_index(0)[1].to_dict()
    try:
        description = driver.find_elements_by_xpath('//div[contains(text(),"Description")]/following-sibling::div')
        driver.execute_script("arguments[0].setAttribute('style','max-height:None;overflow:None;')", description[0])
        productDetails['Description_product'] = description[0].text
    except:
        pass
    productDetails['timestamp'] = str(datetime.datetime.now())
    return productDetails

def getFeatureDetails(driver):
    feature_button = driver.find_elements_by_xpath("//button[text()='View all features']")
    if(len(feature_button)!=0):
        feature_button[0].click()
    details_elements = driver.find_elements_by_css_selector('div.col-12-12 div:nth-child(2)>p')

    featureDetails = {}
    for ftrIdx in range(len(details_elements)):
        featureDetails['feature_' + str(ftrIdx)] = details_elements[ftrIdx].text
    featureDetails['timestamp'] = str(datetime.datetime.now())
    return featureDetails

def getHlEmiDetails(driver):
    OtherDetails = driver.find_elements_by_css_selector('div.col-6-12')
    hl_emi_details = {}
    for hl_emi_idx in range(len(OtherDetails)):
        _detail = OtherDetails[hl_emi_idx].text
        detail_pair = _detail.split('\n',maxsplit = 1)
        if(_detail!='' and len(detail_pair)>1):
            hl_emi_details[detail_pair[0]] = detail_pair[1]
    hl_emi_details['timestamp'] = str(datetime.datetime.now())
    return hl_emi_details


def getASINData(asin,timeout,locator,driver):
    link = 'https://www.flipkart.com/product/p/itme?pid=' + str(asin)

    driver.get(link)
    WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.CSS_SELECTOR, locator)))
    product_title = driver.find_element_by_css_selector('div>div>div>h1').text
    img_list,num_vids = getCatalogContent(driver)
    productDetails = getTechnicalDetails(driver)
    featureDetails = getFeatureDetails(driver)
    hl_emi_details =  getHlEmiDetails(driver)
    
    final_res = {}
    final_res['product title'] = product_title
    final_res['product details'] = productDetails
    final_res['feature details'] = featureDetails
    final_res['hl_emi details'] = hl_emi_details
    final_res['images'] = img_list
    final_res['num vids'] = num_vids
    final_res['timestamp'] = str(datetime.datetime.now())
    return final_res


def getData(asin_list,asin_start_index,timeout,locator,driver):
    asin_page = {}
    current_asin_index = 0
    for asin_idx in tqdm(range(asin_start_index,len(asin_list))):
        try:
            asin = asin_list[asin_idx]
            asin_page[asin] = getASINData(asin,timeout,locator,driver)
            current_asin_index = asin_idx
        except KeyboardInterrupt:
            print('user paused loop')
            break
        except:
            print(sys.exc_info())
            pass
    # print(asin_page)
    return asin_page,current_asin_index