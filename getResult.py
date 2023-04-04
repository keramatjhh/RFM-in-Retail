import pandas as pd
from datetime import datetime 
from sklearn.preprocessing import StandardScaler
import pickle

from os import path
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
import numpy as np

def PredictChurners(start_date:datetime,end_date:datetime,limit:int,monetary_limit:int):

    df = pd.read_parquet("App/data/etka.snappy.parquet")
    
    df_model =  df[(df["InvoiceDate"] > start_date) & ((df["InvoiceDate"] <= end_date))]

    df_g:pd.DataFrame = df_model.groupby("customer_id").agg({'sale':'sum','InvoiceDate':['count','max']})

    # df_g = df_g[df_g[('sale', 'sum')]>monetary_limit]
    
    df_g[('InvoiceDate', 'max')]= (end_date - df_g[('InvoiceDate', 'max')]).dt.days

    df_g.columns = ['M','F','R']
    if monetary_limit is None:
        monetary_limit = df_g["M"].median()
    
    scalar = StandardScaler()
    df_g.loc[:,:] = scalar.fit_transform(df_g)

    model_dir = path.join(path.dirname(__file__),'models')
    
    df_result = df_g.copy()
    # print(df_result)
    with open(path.join(model_dir,"knn.ml"),'rb') as model_file:
        knn_model:KNeighborsClassifier = pickle.load(model_file)

    knn_predict = knn_model.predict_proba(df_g)
    df_result.loc[:,'knn_churn_probability'] = knn_predict[:,1]

    df_result.loc[:,'knn_churn_tag'] = np.where(knn_predict[:,1]> 0.5,True,False)

    with open(path.join(model_dir,"randomForrest.ml"),'rb') as model_file:
        rf_model:RandomForestClassifier = pickle.load(model_file)

    df_result[['M','F','R']] = scalar.inverse_transform(df_result[['M','F','R']])
    
    rf_predict = rf_model.predict_proba(df_g)
    df_result.loc[:,'rf_churn_probability'] = rf_predict[:,1]
    df_result.loc[:,'rf_churn_tag'] = np.where(rf_predict[:,1]> 0.5,True,False)
    
    # df_result.loc[:,"churn_probability"] = df_result[['rf_churn_probability','knn_churn_probability']].max(axis=1)
    df_result.loc[:,"result"] = df_result['rf_churn_tag'].astype(bool)| df_result['knn_churn_tag'].astype(bool)
    df_result.loc[:,"decision"] = (df_result["result"]==True) & (df_result["M"]>monetary_limit) #df_result['rf_churn_tag'].astype(bool)|df_result['knn_churn_tag'].astype(bool)


    df_result["rule_based_result"] = df_result["R"]*df_result["F"]/((end_date-start_date).total_seconds()//(60*60*24)) > 1
    return df_result.reset_index().sort_values(by = 'result',ascending=False).head(limit).to_json(orient='records',indent=2)