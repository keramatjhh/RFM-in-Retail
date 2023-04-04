import pandas as pd
import logging



# 3171838
def GetData(start_date,end_date):
    logger = logging.getLogger()
    df = pd.read_csv(r"D:\Etka\1401\1401.csv",on_bad_lines='skip',usecols=['InvoiceDate','customer_id','sale'],encoding='latin1',nrows=3171838)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'],format="%Y-%m-%d %H:%M:%S.%f")
    df= df[(df['InvoiceDate'] >= start_date) & (df['InvoiceDate'] < end_date)]

    dest_path = "App/data/etka.snappy.parquet"
    df.to_parquet(dest_path,"pyarrow",compression='snappy')
    logger.info(f"Data gathered at file: {dest_path} , count records : {df.shape[0]}")
