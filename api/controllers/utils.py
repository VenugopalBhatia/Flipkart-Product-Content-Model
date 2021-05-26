import os,io
from google.cloud import vision
from google.cloud.vision import types
import json
import numpy as np
import pandas as pd
import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import psycopg2

#####################################################

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Administrator\Desktop\VGB\ScoringContent\ScoringContent_app\controllers\api\credible-art-300905-b1003e20f32c.json'
client = vision.ImageAnnotatorClient()

#####################################################



def detect_text_uri(uri):
    """Detects text in the file located in Google Cloud Storage or on the Web.
    """
    from google.cloud import vision
    client = vision.ImageAnnotatorClient()
    image = types.Image()
    image.source.image_uri = uri
    request = {
        'image':image,
        'features':[{
            'type': vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION
        }
    ,  {
            'type': vision.enums.Feature.Type.OBJECT_LOCALIZATION
    }]
    }
    response = client.annotate_image(request)
    texts = response.text_annotations

    print('Texts:')

    for text in texts:
        print('\n"{}"'.format(text.description))

        vertices = (['({},{})'.format(vertex.x, vertex.y)
                    for vertex in text.bounding_poly.vertices])

        print('bounds: {}'.format(','.join(vertices)))

    if response.error.message:
        raise Exception(response.error.message)
    return response

#####################################################

def getCoordinateTuple(ObjCoordinates,normHeight,normWidth):
    ObjDict = {}
    ObjDict['x'] = []
    ObjDict['y'] = []
    for i in ObjCoordinates:
        ObjDict['x'].append(i.x*normWidth)
        ObjDict['y'].append(i.y*normHeight)
    ObjDict['x'] = tuple(set(ObjDict['x']))
    ObjDict['y'] = tuple(set(ObjDict['y']))
    return ObjDict

#####################################################

def isOverlapping1D(box1,box2):
    return max(box1) >= min(box2) and max(box2) >= min(box1)

#####################################################

def isOverlapping2D(box1,box2):
    return int(isOverlapping1D(box1['x'],box2['x']) and isOverlapping1D(box1['y'],box2['y']))

#####################################################

def getBoxArea(box):
    return (max(box['x']) - min(box['x']))*(max(box['y']) - min(box['y']))

#####################################################

def getImgDetails(url):
    response = detect_text_uri(url)
    imgHeight = response.full_text_annotation.pages[0].height
    imgWidth = response.full_text_annotation.pages[0].width
    word_dict = {}
    for i in range(len(response.text_annotations)):
        word_dict[i] = [getCoordinateTuple(response.text_annotations[i].bounding_poly.vertices,1,1)]
        word_dict[i].append(response.text_annotations[i].description)
    locObj_dict = {}
    for j in range(len(response.localized_object_annotations)):
        locObj_dict[j] = [getCoordinateTuple(response.localized_object_annotations[j].bounding_poly.normalized_vertices,imgHeight,imgWidth)]
        locObj_dict[j].append(response.localized_object_annotations[j].name)
    for word in word_dict.values():
        sum_ = 0
        for obj in locObj_dict.values():
            sum_ += isOverlapping2D(obj[0],word[0])
        if(sum_ >= 1) :
            word.append({"Part of Obj":1})
        else:
            word.append({"Part of Obj":0})
        
    return word_dict,response

#####################################################

def getImgArea(response):
    
    imgHeight = response.full_text_annotation.pages[0].height
    imgWidth = response.full_text_annotation.pages[0].width
    
    return imgHeight*imgWidth,imgHeight,imgWidth

####################################################

# def local_objects_uri(uri):
#     """Localize objects in the image on Google Cloud Storage

#     Args:
#     uri: The path to the file in Google Cloud Storage (gs://...)
#     """
#     response = detect_text_uri(uri)
#     for object_ in response.localized_object_annotations :
#         print(object_)

def getImageFeatures(df_readyprod):
    df_readyprod['aplus_images'] = df_readyprod['aplus_images'].astype(str)
    df_readyprod['aplus_text'] = df_readyprod['aplus_text'].astype(str)
    df_readyprod['imageText'] = np.nan
    df_readyprod['image_WordDict'] = np.nan
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
        image = df_readyprod['aplus_images'][ind]
        try:
            word_dict,response = getImgDetails(image)
            print("****** Response and word dict method ******")
            print(word_dict)
        except:
            word_dict = {}
        areaOfText = 0
        areaOfTextNotInObjects = 0
        characterCount = 0
        characterCountNotInObjects = 0
        averageHeight = 0
        wordCount = 0
        avgCharactersPerUnitArea = 0

        if bool(word_dict):
            
            imgArea = getImgArea(response)
            for idx in range(1,len(word_dict.keys())):
                reqArea = getBoxArea(word_dict[idx][0])
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
            df_readyprod['imageText'][ind] = word_dict[0][1]
        except:
            df_readyprod['imageText'][ind] = "N/A"
        try:
            # df_readyprod['Objects'][ind] = []
            for obj_ in response.localized_object_annotations:
                if(obj_.name == "Person"):
                    df_readyprod['Person Detected'][ind] = True
                # df_readyprod['Objects'][ind].append([obj_.name,obj_.score])
                # print("**** Object added ")
        except:
            pass
        df_readyprod['image_WordDict'][ind] = word_dict
        df_readyprod['image_textArea'][ind] = areaOfText  
        df_readyprod['image_nonObjectTextArea'][ind] = areaOfTextNotInObjects
        df_readyprod['image_textArea_percent'][ind] = areaOfText/imgArea[0] 
        df_readyprod['image_nonObjectTextArea_percent'][ind] = areaOfTextNotInObjects/imgArea[0]
        df_readyprod['image_height'][ind] = imgArea[1]
        df_readyprod['image_width'][ind] = imgArea[2]
        df_readyprod['image_character_count'][ind] = characterCount
        df_readyprod['image_character_count_nonObject'][ind] = characterCountNotInObjects
        df_readyprod['average_text_height'][ind] = averageHeight
        df_readyprod['average_characters_per_unit_area'][ind] = avgCharactersPerUnitArea
        print(df_readyprod.iloc[ind])

    df_readyprod['character_count'] = df_readyprod.aplus_text.str.len()
    df_readyprod['textHeight/imageHeight'] = df_readyprod['average_text_height']/df_readyprod['image_height']
    df_readyprod['image_area']= df_readyprod['image_height']*df_readyprod['image_width']

