import pandas as pd
import ast


def ImageScoring(ScoringParameters,ImageFile,sheetName,imgType):
    df_images = pd.read_excel(ImageFile,index_col = 0, engine = 'openpyxl')
    df_imgScoring = pd.read_excel(ScoringParameters,sheet_name=sheetName,index_col = 0, engine = 'openpyxl')
    imgTypeKey = imgType + "_images"
    imgTypeScoring = "avg" + imgType + "Score"
    textHeightLR = df_imgScoring.loc['Less than 1 image with text height out of range','Value'].split("-")[0]
    textHeightLR = int(textHeightLR)/100
    textHeightUR = df_imgScoring.loc['Less than 1 image with text height out of range','Value'].split("-")[1]
    textHeightUR = int(textHeightUR)/100
    textPercent = df_imgScoring.loc["Less than 1 image with Text % higher than",'Value']/100
    numImageVal_0 = df_imgScoring.loc["Number of Images less than",'Value'][0]
    numImageVal_1 = df_imgScoring.loc["Number of Images less than",'Value'][1]

    numImagesScoreVal_0 = df_imgScoring.loc["Number of Images less than",'Score'][0]
    numImagesScoreVal_1 = df_imgScoring.loc["Number of Images less than",'Score'][1]
    TextPercentScoreVal1 = df_imgScoring.loc["Less than 1 image with Text % higher than",'Score']
    TextPercentScoreVal2 = df_imgScoring.loc["Less than 2 image with Text % higher than",'Score']
    TextPercentScoreVal3 = df_imgScoring.loc["Less than 3 image with Text % higher than",'Score']
    TextHeightScoreVal1 = df_imgScoring.loc['Less than 1 image with text height out of range','Score']
    TextHeightScoreVal2 = df_imgScoring.loc['Less than 2 image with text height out of range','Score']
    TextHeightScoreVal3 = df_imgScoring.loc['Less than 3 image with text height out of range','Score']

    df_images['aboveTextPercentThreshold'] = False
    df_images.loc[df_images['image_nonObjectTextArea_percent']>textPercent,'aboveTextPercentThreshold'] = True

    df_images.loc[df_images['image_nonObjectTextArea_percent']>0,'textInImage'] = True
    df_asinImageScore = df_images.groupby('channel_sku_id')['aboveTextPercentThreshold'].apply(lambda x: (x==True).sum()).reset_index(name = 'numImagesTextAreaPercentCrossed')
    df_asinImageScore['numImages'] = df_images.groupby('channel_sku_id')[imgTypeKey].count().values

    df_asinImageScore.loc[df_asinImageScore['numImagesTextAreaPercentCrossed']<=3,'TextPercentScore'] = TextPercentScoreVal3
    df_asinImageScore.loc[df_asinImageScore['numImagesTextAreaPercentCrossed']<=2,'TextPercentScore'] = TextPercentScoreVal2
    df_asinImageScore.loc[df_asinImageScore['numImagesTextAreaPercentCrossed']<=1,'TextPercentScore'] = TextPercentScoreVal1
    
    df_asinImageScore['TextPercentScore'].fillna(0,inplace = True)
    df_asinImageScore['numImagesScore'] = 100
    df_asinImageScore.loc[df_asinImageScore['numImages']<numImageVal_1,'numImagesScore'] = numImagesScoreVal_1
    df_asinImageScore.loc[df_asinImageScore['numImages']<numImageVal_0,'numImagesScore'] = numImagesScoreVal_0

    df_images['textHeight/imageHeight'].fillna(0,inplace = True)
    df_images['inTextHeightRange'] = False
    df_images.loc[df_images['textHeight/imageHeight'].between(textHeightLR,textHeightUR),'inTextHeightRange'] = True
    
    df_asinImageScore['numImagesOutOfTextHeightRange'] = df_images.groupby('channel_sku_id')['inTextHeightRange'].apply(lambda x: (x==False).sum()).reset_index(name = 'numImagesOutOfTextHeightRange')['numImagesOutOfTextHeightRange']
    df_asinImageScore.loc[df_asinImageScore['numImagesOutOfTextHeightRange']<=3,'TextHeightScore'] = TextHeightScoreVal3
    df_asinImageScore.loc[df_asinImageScore['numImagesOutOfTextHeightRange']<=2,'TextHeightScore'] = TextHeightScoreVal2
    df_asinImageScore.loc[df_asinImageScore['numImagesOutOfTextHeightRange']<=1,'TextHeightScore'] = TextHeightScoreVal1
    df_asinImageScore['TextHeightScore'].fillna(0,inplace = True)

    df_asinImageScore[imgTypeScoring] = df_asinImageScore[['TextPercentScore','numImagesScore','TextHeightScore']].mean(axis = 1)
    
    return df_asinImageScore


def ProductTitleScoring(ScoringParameters,ProductTitleFile,sheetName_Scoring,sheetName_Data):
    df_productTitle = pd.read_excel(ProductTitleFile,sheet_name=sheetName_Data,index_col=0,engine = 'openpyxl')
    df_productTitleScoring = pd.read_excel(ScoringParameters,sheet_name= sheetName_Scoring,index_col=0, engine = 'openpyxl')

    numChars_1LR = df_productTitleScoring.loc["Number of characters_1","Range_lower"]
    numChars_1HR = df_productTitleScoring.loc["Number of characters_1","Range_higher"]
    numChars_2LR = df_productTitleScoring.loc["Number of characters_2","Range_lower"]
    numChars_2HR = df_productTitleScoring.loc["Number of characters_2","Range_higher"]
    numChars_3LR = df_productTitleScoring.loc["Number of characters_3","Range_lower"]
    numChars_3HR = df_productTitleScoring.loc["Number of characters_3","Range_higher"]
    numChars_4LR = df_productTitleScoring.loc["Number of characters_4","Range_lower"]
    numChars_4HR = df_productTitleScoring.loc["Number of characters_4","Range_higher"]
    keywordAvailability_1LR = df_productTitleScoring.loc["Keyword Availability_1","Range_lower"]
    keywordAvailability_1HR = df_productTitleScoring.loc["Keyword Availability_1","Range_higher"]
    keywordAvailability_2LR = df_productTitleScoring.loc["Keyword Availability_2","Range_lower"]
    keywordAvailability_2HR = df_productTitleScoring.loc["Keyword Availability_2","Range_higher"]
    keywordAvailability_3LR = df_productTitleScoring.loc["Keyword Availability_3","Range_lower"]
    keywordAvailability_3HR = df_productTitleScoring.loc["Keyword Availability_3","Range_higher"]
    keyWordAvailabilityFirstEighty_1LR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_lower"][0]
    keyWordAvailabilityFirstEighty_1HR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_higher"][0]
    keyWordAvailabilityFirstEighty_2LR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_lower"][1]
    keyWordAvailabilityFirstEighty_2HR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_higher"][1]
    keyWordAvailabilityFirstEighty_3LR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_lower"][2]
    keyWordAvailabilityFirstEighty_3HR = df_productTitleScoring.loc["Keywords in first 80 characters","Range_higher"][2]


    numChars_1Score = df_productTitleScoring.loc["Number of characters_1","Score"]
    numChars_2Score = df_productTitleScoring.loc["Number of characters_2","Score"]
    numChars_3Score = df_productTitleScoring.loc["Number of characters_3","Score"]
    numChars_4Score = df_productTitleScoring.loc["Number of characters_4","Score"]
    keywordAvailability_1Score = df_productTitleScoring.loc["Keyword Availability_1","Score"]
    keywordAvailability_2Score = df_productTitleScoring.loc["Keyword Availability_2","Score"]
    keywordAvailability_3Score = df_productTitleScoring.loc["Keyword Availability_3","Score"]
    keyWordAvailabilityFirstEighty_1Score = df_productTitleScoring.loc["Keywords in first 80 characters","Score"][0]
    keyWordAvailabilityFirstEighty_2Score = df_productTitleScoring.loc["Keywords in first 80 characters","Score"][1]
    keyWordAvailabilityFirstEighty_3Score = df_productTitleScoring.loc["Keywords in first 80 characters","Score"][2]


    df_productTitle.loc[df_productTitle['title word count'].between(numChars_1LR,numChars_1HR),'numChars_Score'] = numChars_1Score
    df_productTitle.loc[df_productTitle['title word count'].between(numChars_2LR,numChars_2HR),'numChars_Score'] = numChars_2Score
    df_productTitle.loc[df_productTitle['title word count'].between(numChars_3LR,numChars_3HR),'numChars_Score'] = numChars_3Score
    df_productTitle.loc[df_productTitle['title word count'].between(numChars_4LR,numChars_4HR),'numChars_Score'] = numChars_4Score
    df_productTitle['keywordAvailability_Score'] = 0
    df_productTitle.loc[df_productTitle['Match_Score'].between(keywordAvailability_1LR,keywordAvailability_1HR),'keywordAvailability_Score'] = keywordAvailability_1Score
    df_productTitle.loc[df_productTitle['Match_Score'].between(keywordAvailability_2LR,keywordAvailability_2HR),'keywordAvailability_Score'] = keywordAvailability_2Score
    df_productTitle.loc[df_productTitle['Match_Score'].between(keywordAvailability_3LR,keywordAvailability_3HR),'keywordAvailability_Score'] = keywordAvailability_3Score
    df_productTitle['keywordAvailabilityFirstEighty_Score'] = 0
    df_productTitle.loc[df_productTitle['Match_Score_firstEighty'].between(keyWordAvailabilityFirstEighty_1LR,keyWordAvailabilityFirstEighty_1HR),'keywordAvailabilityFirstEighty_Score'] = keyWordAvailabilityFirstEighty_1Score
    df_productTitle.loc[df_productTitle['Match_Score_firstEighty'].between(keyWordAvailabilityFirstEighty_2LR,keyWordAvailabilityFirstEighty_2HR),'keywordAvailabilityFirstEighty_Score'] = keyWordAvailabilityFirstEighty_2Score
    df_productTitle.loc[df_productTitle['Match_Score_firstEighty'].between(keyWordAvailabilityFirstEighty_3LR,keyWordAvailabilityFirstEighty_3HR),'keywordAvailabilityFirstEighty_Score'] = keyWordAvailabilityFirstEighty_3Score

    df_productTitle['spelling_errors_flags'] = df_productTitle['spelling_errors_flags'].apply(lambda x: ast.literal_eval(x))
    df_productTitle['grammarCheck_Score'] = 100
    for i in df_productTitle.index:
        dict_ = df_productTitle['spelling_errors_flags'][i]
        for j in dict_.values():
            if(j==1):
                df_productTitle.at[i,'grammarCheck_Score'] = 0
    
    df_productTitle['duplicateWordCountScore'] = 100
    df_productTitle.loc[df_productTitle['duplicate word count'] != 0,'duplicateWordCountScore'] = 0
    df_productTitle['avgProductTitleScore'] = df_productTitle[['duplicateWordCountScore','grammarCheck_Score','keywordAvailabilityFirstEighty_Score','keywordAvailability_Score','numChars_Score']].mean(axis = 1)
    # df_productTitle.drop_duplicates(inplace=True)
    df_ProductTitle_Agg = df_productTitle.groupby('asin',as_index = False)[['numChars_Score','keywordAvailability_Score','keywordAvailabilityFirstEighty_Score','grammarCheck_Score','duplicateWordCountScore','avgProductTitleScore']].mean()

    return df_productTitle,df_ProductTitle_Agg


def ProductFeatureCheck(ScoringParameters,FeatureCheckFile,sheetName_Scoring,sheetName_Data):
    df_productDescription = pd.read_excel(FeatureCheckFile,sheet_name= sheetName_Data,index_col=0,engine = 'openpyxl')
    df_productDescriptionScoring = pd.read_excel(ScoringParameters,sheet_name= sheetName_Scoring,index_col = 0, engine = 'openpyxl')

    df_productDescription['Word Count'] = df_productDescription['Combined Features'].str.split(" ").apply(lambda x: len(x))
    df_productDescription['featurePresentPercent'] = df_productDescription['featureCount']/df_productDescription['totalFeatures']
    df_productDescription['featurePresentPercent'] = df_productDescription['featurePresentPercent']*100

    numWords_1LR = df_productDescriptionScoring.loc["Number of words_1","Range_lower"]
    numWords_1HR = df_productDescriptionScoring.loc["Number of words_1","Range_higher"]
    numWords_2LR = df_productDescriptionScoring.loc["Number of words_2","Range_lower"]
    numWords_2HR = df_productDescriptionScoring.loc["Number of words_2","Range_higher"]
    numWords_3LR = df_productDescriptionScoring.loc["Number of words_3","Range_lower"]
    numWords_3HR = df_productDescriptionScoring.loc["Number of words_3","Range_higher"]

    keywordAvailability_1LR = df_productDescriptionScoring.loc["Keyword Availability_1","Range_lower"]
    keywordAvailability_1HR = df_productDescriptionScoring.loc["Keyword Availability_1","Range_higher"]
    keywordAvailability_2LR = df_productDescriptionScoring.loc["Keyword Availability_2","Range_lower"]
    keywordAvailability_2HR = df_productDescriptionScoring.loc["Keyword Availability_2","Range_higher"]
    keywordAvailability_3LR = df_productDescriptionScoring.loc["Keyword Availability_3","Range_lower"]
    keywordAvailability_3HR = df_productDescriptionScoring.loc["Keyword Availability_3","Range_higher"]

    numWords_1Score = df_productDescriptionScoring.loc["Number of words_1","Score"]
    numWords_2Score = df_productDescriptionScoring.loc["Number of words_2","Score"]
    numWords_3Score = df_productDescriptionScoring.loc["Number of words_3","Score"]


    keywordAvailability_1Score = df_productDescriptionScoring.loc["Keyword Availability_1","Score"]
    keywordAvailability_2Score = df_productDescriptionScoring.loc["Keyword Availability_2","Score"]
    keywordAvailability_3Score = df_productDescriptionScoring.loc["Keyword Availability_3","Score"]

    df_productDescription.loc[df_productDescription['Word Count'].between(numWords_1LR,numWords_1HR),'numWords_Score'] = numWords_1Score
    df_productDescription.loc[df_productDescription['Word Count'].between(numWords_2LR,numWords_2HR),'numWords_Score'] = numWords_2Score
    df_productDescription.loc[df_productDescription['Word Count'].between(numWords_3LR,numWords_3HR),'numWords_Score'] = numWords_3Score

    df_productDescription.loc[df_productDescription['featurePresentPercent'].between(keywordAvailability_1LR,keywordAvailability_1HR),'keywordAvailability_Score'] = keywordAvailability_1Score
    df_productDescription.loc[df_productDescription['featurePresentPercent'].between(keywordAvailability_2LR,keywordAvailability_2HR),'keywordAvailability_Score'] = keywordAvailability_2Score
    df_productDescription.loc[df_productDescription['featurePresentPercent'].between(keywordAvailability_3LR,keywordAvailability_3HR),'keywordAvailability_Score'] = keywordAvailability_3Score

    df_productDescription['averageProductDescriptionScore'] = df_productDescription[['featurePresentPercent','numWords_Score','keywordAvailability_Score']].mean(axis = 1)
    
    df_productDescription.drop_duplicates(inplace = True)

    return df_productDescription


