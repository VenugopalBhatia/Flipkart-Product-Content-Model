import pandas as pd
import numpy as np
import sys
import psycopg2
import utils
from tqdm import tqdm

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

def getAplusImageData(asinTuple):
    pg = postgres_conn()
    conn = pg.getConn()
    

    query_readyprod = f""" 
    Select DISTINCT a.brand, a.channel_sku_id,b.aplus_images,b.aplus_text from entity.product_feature_mapping a left join 
    ready.ready_product_details b  on a.channel_sku_id = b.asin where b.asin in {asinTuple} """

    df_readyprod = pd.read_sql_query(query_readyprod, conn[1])
    df_readyprod = df_readyprod.explode('aplus_images',ignore_index=True)
    df_readyprod.dropna(inplace= True)
    # df_readyprod.astype(str).drop_duplicates(inplace=True)
    return df_readyprod


def getImagesDetails(df,asinImageFile,imgType):
    # asinImageFile  = os.path
    df_readyprod = df.copy()
    imgTypeKey = imgType + '_images'
    imgTypeTxtKey = imgType + '_text'
    df_readyprod[imgTypeKey] = df_readyprod[imgTypeKey].astype(str)
    try:
        df_readyprod[imgTypeTxtKey] = df_readyprod[imgTypeTxtKey].astype(str)
    except:
        pass
    df_readyprod['imageText'] = ""
    df_readyprod['image_WordDict'] = ""
    df_readyprod['image_textArea'] = np.nan
    df_readyprod['image_nonObjectTextArea'] = np.nan
    df_readyprod['image_textArea_percent'] = np.nan  
    df_readyprod['image_nonObjectTextArea_percent'] = np.nan
    df_readyprod['image_height'] = np.nan
    df_readyprod['image_width'] = np.nan
    df_readyprod['image_character_count'] = np.nan
    df_readyprod['image_character_count_nonObject'] = np.nan
    df_readyprod['average_text_height'] = np.nan
    df_readyprod['average_characters_per_unit_area'] = np.nan
    df_readyprod['Person Detected'] = np.nan
    df_readyprod['Objects'] = np.nan

    for ind in tqdm(df_readyprod.index):
        image = df_readyprod[imgTypeKey][ind]
        print(image)
        try:
            word_dict,response = utils.getImgDetails(image)
            print(word_dict)
        except:
            print("*******",sys.exc_info())
            word_dict = {}
        
        areaOfText = 0
        areaOfTextNotInObjects = 0
        characterCount = 0
        characterCountNotInObjects = 0
        averageHeight = 0
        wordCount = 0
        avgCharactersPerUnitArea = 0
        if bool(word_dict):
            
            imgArea = utils.getImgArea(response)
            for idx in range(1,len(word_dict.keys())):
                reqArea = utils.getBoxArea(word_dict[idx][0])
                textHeight = max(word_dict[idx][0]['y'])-min(word_dict[idx][0]['y'])
                textHeight_dict = {"Height":textHeight}
                reqArea_dict = {"TextArea":reqArea}
                word_dict[idx].append(textHeight_dict)
                word_dict[idx].append(reqArea_dict)
                areaOfText+=reqArea
                word = word_dict[idx][1]
                characterCount+=len(word)
                if(word_dict[idx][2]['Part of Obj']!=1):
                    wordCount += 1
                    areaOfTextNotInObjects += reqArea
                    characterCountNotInObjects += len(word)
                    try:
                        avgCharactersPerUnitArea += (len(word)/reqArea)
                    except:
                        pass
                    averageHeight += word_dict[idx][3]['Height']
            try:
                averageHeight /= wordCount
                avgCharactersPerUnitArea /= wordCount
            except:
                pass
            
                
        else:
            imgArea = (-np.inf,0,0)
        try:
            df_readyprod.at[ind,'imageText'] = word_dict[0][1]
        except:
            df_readyprod.at[ind,'imageText'] = "N/A"
        try:
            # df_readyprod['Objects'][ind] = []
            for obj_ in response.localized_object_annotations:
                if(obj_.name == "Person"):
                    df_readyprod.at[ind,'Person Detected'] = True
                # df_readyprod['Objects'][ind].append([obj_.name,obj_.score])
                # print("**** Object added ")
        except:
            pass
        df_readyprod.at[ind,'image_WordDict'] = str(word_dict)
        df_readyprod.at[ind,'image_textArea'] = areaOfText  
        df_readyprod.at[ind,'image_nonObjectTextArea'] = areaOfTextNotInObjects
        df_readyprod.at[ind,'image_textArea_percent'] = areaOfText/imgArea[0] 
        df_readyprod.at[ind,'image_nonObjectTextArea_percent'] = areaOfTextNotInObjects/imgArea[0]
        df_readyprod.at[ind,'image_height'] = imgArea[1]
        df_readyprod.at[ind,'image_width'] = imgArea[2]
        df_readyprod.at[ind,'image_character_count'] = characterCount
        df_readyprod.at[ind,'image_character_count_nonObject'] = characterCountNotInObjects
        df_readyprod.at[ind,'average_text_height'] = averageHeight
        df_readyprod.at[ind,'average_characters_per_unit_area'] = avgCharactersPerUnitArea

    try:
        df_readyprod['character_count'] = df_readyprod[imgTypeTxtKey].str.len()
    except:
        pass
    df_readyprod['textHeight/imageHeight'] = df_readyprod['average_text_height']/df_readyprod['image_height']
    df_readyprod['image_area']= df_readyprod['image_height']*df_readyprod['image_width']
    df_readyprod.to_excel(asinImageFile)
    return df_readyprod
