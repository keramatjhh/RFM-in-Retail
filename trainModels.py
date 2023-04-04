
from sklearn.model_selection import train_test_split

import pandas as pd
from datetime import timedelta,datetime
from sklearn.preprocessing import StandardScaler
import logging

from scripts.churn.models.svm import SVMModel
from scripts.churn.models.knn import KNNModel
from scripts.churn.models.randomForrest import RFModel

import logging




def Train(model:str,model_start_date: datetime,model_end_date:datetime,model_churn_boundry:int):
    logger = logging.getLogger()


    df_final = PrepareData(model_start_date,model_end_date,churn_boundry=model_churn_boundry)
    # logger.info(df_final.columns)
    features_train,features_test,flag_train,flag_test = train_test_split(df_final.drop('churn',axis=1),df_final['churn'],test_size=0.2)

    if model == 'KNN':
        result = KNNModel(features_train=features_train,
                        flags_train=flag_train,
                        features_test= features_test,
                        flags_test= flag_test)
                        
    elif model == 'RandomForrest':
        result = RFModel(features_train=features_train,
                        flags_train=flag_train,
                        features_test= features_test,
                        flags_test= flag_test)
    return result
def PrepareData(start_date : datetime, end_date : datetime,churn_boundry: int) -> pd.DataFrame:
    logger = logging.getLogger()
    df = pd.read_parquet("App/data/etka.snappy.parquet")

    churn_date = end_date + timedelta(days = churn_boundry)
    
    lastDate = df["InvoiceDate"].max()

    df_churn = df[(df["InvoiceDate"] >=  end_date) & (df["InvoiceDate"] <=  churn_date)].groupby("customer_id").size()
    df_churn.name = 'churn'

    df_model =  df[(df["InvoiceDate"] > start_date) & ((df["InvoiceDate"] <= end_date))]

    df_g:pd.DataFrame = df_model.groupby("customer_id").agg({'sale':'sum','InvoiceDate':['count','max']})#,'count']})
    
    df_g[('InvoiceDate', 'max')]= (end_date - df_g[('InvoiceDate', 'max')]).dt.days

    # df_g = df_g[df_g[('sale', 'sum')] > monetary_limit]

    df_g.loc[:,:] = StandardScaler().fit_transform(df_g)


    df_final = df_g.join(df_churn,how='left')
    df_final.loc[df_final.churn.isna(),'churn'] = 0
    df_final.loc[df_final.churn>=1 ,'churn'] = 1
        
    return df_final