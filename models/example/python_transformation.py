# import snowflake.snowpark.functions as f 
# # from snowflake.snowpark.functions import col

import json
import pandas as pd
import numpy as np



def transformation_function(row):
    try:
        df = pd.json_normalize(json.loads(row[0]))
        # print(type(row[0]))       
        df1 = df.iloc[0]
        return df1    
    except:
        return row



def model(dbt, session):

    dbt.config(packages= ['pandas'])
    data_df = dbt.ref("source_data").to_pandas()
    meta_df = dbt.ref("transformation_meta").to_pandas()

    list_01 = [] 
    final_df = pd.DataFrame()

    df = data_df    
    column_headers = list(data_df.columns.values)

    for items in column_headers:
        temp = items
        source_df = (df[[temp]]) 
        # print(type(source_df))    
        new_list = source_df.apply(transformation_function, axis=1) 
        list_01.append(new_list)
        # print ((new_list))


    # print(type(list_01[0]))

    for dfs in list_01:
        final_df = pd.concat([final_df, dfs], axis=1)

    # column_names = (meta_df["GOOGLE"])

    # final_df.rename(columns = column_names, inplace = True)


    return final_df
    