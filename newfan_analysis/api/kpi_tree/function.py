import os
from pandas import DataFrame
import pandas as pd
import numpy as np
from dateutil.parser import parse
from dotenv import load_dotenv
load_dotenv()

import boto3
import mysql.connector as mysql
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import create_engine


s3 = boto3.resource('s3',
        aws_access_key_id=os.environ.get("aws_access_key_id"),
        aws_secret_access_key=os.environ.get("aws_secret_access_key"),
        region_name='ap-northeast-1'
)
s3_client = boto3.client('s3',
        aws_access_key_id=os.environ.get("aws_access_key_id"),
        aws_secret_access_key=os.environ.get("aws_secret_access_key"),
        region_name='ap-northeast-1'
)
BUCKET_NAME = 'newfan-analysis'


# ========================
# Tools
# ========================
def func_create_user_database(uid):
    conn = mysql.connect(
        host="db"
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {uid}")
    cursor.close()
    conn.close()
    result = {"status": "success",
              "database":uid}
    return result


def _get_data_s3(uid, table):
    bucket = s3.Bucket(BUCKET_NAME)
    prefix = "UPLOAD_DATA"+"/"+uid
    objects = [obj.key for obj in bucket.objects.filter(Prefix=prefix).all()]
    get_items = [item.split("/")[3] for item in objects if table in item]
    get_items.sort(reverse=True)
    return get_items

def _insert_batch_data(df, table, conn):
    num = 100000
    try:
        for i in range(int(df.shape[0]/num)+1):
            if i == 0:
                tmp_df = df[:num]
                tmp_df.to_sql(table, conn, index=False, if_exists='append')
            else:
                from_num = i*num
                to_num = (i+1)*num
                tmp_df = df[from_num:to_num]
                tmp_df.to_sql(table, conn, index=False, if_exists='append')
        return "Success"
    except Exception as e:
        return e

def func_create_table(uid, table):
    DB_URL = f"mysql+mysqlconnector://root@db:3306/{uid}?charset=utf8"
    engine = create_engine(DB_URL, echo=True)

    s3_data_all = _get_data_s3(uid, table)
    for s3_data in s3_data_all:
        with engine.begin() as conn:
            key = 'UPLOAD_DATA/'+uid+'/'+table+'/'+s3_data
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(obj['Body'])
            _insert_batch_data(df, table, conn)
    return f"{table} created!!"


def _get_table_data(uid, table_name):
    """
    Output data in DataFrame type by entering table name
    """
    DB_URL = f"mysql+mysqlconnector://root@db:3306/{uid}?charset=utf8"
    engine = create_engine(DB_URL, echo=True)

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, engine)

    return df

def _create_period(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column], format="%Y-%m-%d")
    df["year"] = pd.to_datetime(df[date_column], format="%Y-%m-%d").dt.to_period("Y")
    df["quarter"] = pd.to_datetime(df[date_column], format="%Y-%m-%d").dt.to_period("Q")
    df["month"] = pd.to_datetime(df[date_column], format="%Y-%m-%d").dt.to_period("M")
    df["week"] = pd.to_datetime(df[date_column], format="%Y-%m-%d").dt.to_period("W")

    return df


# 集計のためのヘルパー関数
def _func_period_calc(df: DataFrame,
                      period: str,
                      amount_column: str,
                      axis=None):

    if axis != "":
        calc = df[[period, axis, amount_column]].groupby([period, axis])
    else:
        calc = df[[period, amount_column]].groupby(period)
    return calc


def _calc(df: DataFrame,
          period: str,
          kpi_column: str,
          method: str,
          axis=None):
    """
    Defines the process of calculating the KPI; data about the KPI item and the period covered,
    The process receives data related to the KPI item, the period covered, the item name for the date,
    the name of the target KPI item, and the calculation method, and performs the calculation.
    """
    calc = _func_period_calc(df, period, kpi_column, axis)
    if method == 'sum':
        output = calc.sum().reset_index()
    elif method == 'count':
        output = calc.count().reset_index()
    elif method == 'mean':
        output = calc.mean().reset_index()
    elif method == 'unique':
        output = calc.nunique().reset_index()
    else:
        output = calc.count().reset_index()
    return output


def func_calc_kpi(df,
             period,
             kpi_column,
             method,
             terms_column,
             terms,
             axis):
    """
    Defines the process of calculating the KPI; data about the KPI item and the period covered,
    The process receives data related to the KPI item, the period covered, the item name for the date,
    the name of the target KPI item, and the calculation method, and performs the calculation.
    Unlike normal KPI calculations, the necessary conditions are specified.
    """
    if (terms_column == "") & (terms == ""):
        if (type(terms_column) == list) & (type(terms) == list):
            for term_col, term in zip(terms_column, terms):
                df = df[df[term_col] == term]
        else:
            df = df[df[terms_column] == terms]
    output = _calc(df, period, kpi_column, method, axis)
    output = output.rename(columns={kpi_column: 'output'})
    return output


def func_calc_kpi_top_bottom(df,
                        period,
                        top_column,
                        bottom_column,
                        method_top,
                        method_bottom,
                        terms_column,
                        terms,
                        axis):
    """
    Defines the process of calculating the KPI; data about the KPI item and the period covered,
    The calculation is performed upon receiving the data related to the KPI item, the period under consideration,
    the item name for the date, the numerator, the denominator, and the calculation method.
    In addition, specify any necessary conditions.
    """
    top = func_calc_kpi(df, period, top_column, method_top, terms_column, terms, axis)
    bottom = func_calc_kpi(df, period, bottom_column, method_bottom, terms_column, terms, axis)
    top["output"] = top[top.columns[-1]] / bottom[bottom.columns[-1]]

    if axis is not None:
        return top[[period, axis, "output"]]
    else:
        return top[[period, "output"]]


def create_columns_new_repeat_flg(df, period, user_id_column):
    calc_df = df[[user_id_column, period]].drop_duplicates()
    calced = calc_df.groupby(user_id_column).min().reset_index()
    calced["new_user_"+period] = "新規"

    df = pd.merge(df, calced, on=[user_id_column, period], how='left')
    df["new_user_"+period] = df["new_user_"+period].fillna("リピート")
    return df


def func_calc_indicator(df, indicator_dic):
    functions = {"calc_kpi": func_calc_kpi,
                 "calc_kpi_top_bottom": func_calc_kpi_top_bottom}
    get_func = indicator_dic.get("function")
    func = functions[get_func]
    kpi = indicator_dic.get("kpi")
    period = indicator_dic.get("period")
    input_column = indicator_dic.get("input_column")
    method = indicator_dic.get("method")
    terms_column = indicator_dic.get("terms_column")
    terms = indicator_dic.get("term")
    axis = indicator_dic.get("axis")

    indicator = func(df, period, input_column, method, terms_column, terms, axis)
    indicator["KPI"] = kpi
    indicator["Increase/Decrease"] = indicator["output"].diff() / np.append(1, indicator["output"].values[:-1])
    indicator[period] = indicator[period].astype(str)
    return indicator


def func_calc_indicator_kpi(uid, date_column, user_id_column, indicator_dic_list:list[dict[str, str]]):
    output = pd.DataFrame()
    DB_URL = f"mysql+mysqlconnector://root@db:3306/{uid}?charset=utf8"
    engine = create_engine(DB_URL, echo=True)
    with engine.begin() as conn:
        df = pd.read_sql_query("SELECT * FROM SALES", conn)
        df = _create_period(df, date_column)
        for period in ["year", "quarter", "month", "week"]:
            df = create_columns_new_repeat_flg(df, period, user_id_column)
        for indicator_dic in indicator_dic_list:
            out_df = func_calc_indicator(df, indicator_dic)
            output = pd.concat([output, out_df])

        output.to_sql("KPI_TREE", conn, index=False, if_exists='append')


def func_get_mysql(uid):
    DB_URL = f"mysql+mysqlconnector://root@db:3306/{uid}?charset=utf8"
    engine = create_engine(DB_URL, echo=True)
    with engine.begin() as conn:
        df = pd.read_sql_query("SELECT * FROM KPI_TREE;", conn)
    return df.to_json(orient='records', force_ascii=False)