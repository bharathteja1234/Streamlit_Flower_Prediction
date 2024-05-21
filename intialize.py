from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, FloatType
from snowflake.snowpark.functions import avg,sum, col, udf, call_udf, call_builtin, year, month
import streamlit as st
import pandas as pd
from datetime import date

from sklearn.linear_model import LinearRegression

connection_parameters= {
    "account":"admin",
    "user":"bharath",
    "password":"user123",
    "warehouse":"compute_wh",
    "role":"accountadmin",
    "databbase":"summit_hol",
    "schema":"public"
}
secession = Secession.builder.configs(connection_parameters).create()
secession.sql("select current_account() acct, current_warehouse() wh, current_database() db, current_schema() schema, current_version() v").show()
session.sql("SELECT DISTINCT variable_name FROM CYBERSYN_FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FINANCIAL_FRED_TIMESERIES").show()

session.sql("SELECT COUNT(*) FROM CYBERSYN_FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FINANCIAL_FRED_TIMESERIES").show()
snow_df_pce = (session.table("CYBERSYN_FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FINANCIAL_FRED_TIMESERIES")
               .filter(col('VARIABLE_NAME') == 'Personal Consumption Expenditures: Chain-type Price Index, Seasonally adjusted, Monthly, Index 2012=100')
               .filter(col('DATE') >= '1972-01-01')
               .filter(month(col('DATE')) == 1)
               .orderBy(col('DATE'))) 
snow_df_pce.show()
snow_df_pce = (session.table("CYBERSYN_FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FINANCIAL_FRED_TIMESERIES")
               .filter(col('VARIABLE_NAME') == 'Personal Consumption Expenditures: Chain-type Price Index, Seasonally adjusted, Monthly, Index 2012=100')
               .filter(col('DATE') >= '1972-01-01')
               .filter(month(col('DATE')) == 1))
pd_df_pce_year = snow_df_pce.select(year(col('DATE')).alias('"Year"'), col('VALUE').alias('PCE')).orderBy(col('DATE')).to_pandas()
pd_df_pce_year
x = pd_df_pce_year["Year"].to_numpy().reshape(-1,1)
y = pd_df_pce_year["PCE"].to_numpy()

model = LinearRegression().fit(x, y)

# test model for 2022
predictYear = 2022
pce_pred = model.predict([[predictYear]])
# print the last 5 years
print (pd_df_pce_year.tail() )
# run the prediction for 2022
print ('Prediction for '+str(predictYear)+': '+ str(round(pce_pred[0],2)))
def predict_pce(predictYear: int) -> float:
   return model.predict([[predictYear]])[0].round(2).astype(float)

_ = session.udf.register(predict_pce,
                       return_type=FloatType(),
                       input_type=IntegerType(),
                       packages= ["pandas","scikit-learn"],
                       is_permanent=True,
                       name="predict_pce_udf",
                       replace=True,
                       stage_location="@udf_stage")
session.sql("select predict_pce_udf(2022)").show()
