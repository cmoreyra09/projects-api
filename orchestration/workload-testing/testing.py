import pandas as pd
import sys 
sys.path.insert(0,r'../../db_configuration/')
from connectiondb import sqlConnectDB

db = sqlConnectDB('localhost', 'RawDataLake')
engine = db.connectDb()

df = pd.read_csv(r"C:\Users\Carlos\Downloads\BBDA_FORECAST_NICE.txt",sep='|',encoding='latin1')

df.to_csv(r"C:\Users\Carlos\Downloads\BBDA_FORECAST_NICE.txt",index=False,sep='|')
print(df.head(5))

## 25 - 121 to float

##for col in df.columns[25:121]:
##    df[col] = df[col].astype(float)

##print(df.dtypes)

##df.to_sql('TBL_TESTING_NICE', con=engine, if_exists='replace', index=False)


## DISTRIBUCION FLOAT
## PERMISO_C_GOCE  - COSTO NOMINA FLOAT 