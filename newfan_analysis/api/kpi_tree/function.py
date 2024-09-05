import os
from pandas import DataFrame
import pandas as pd
from dateutil.parser import parse
from dotenv import load_dotenv
load_dotenv()

import boto3
import mysql
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
    conn = mysql.connector.connect(
        host="db"
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {uid}")
    cursor.close()
    conn.close()
    result = {"status": "success",
              "database":uid}
    return result


def _get_data_s3(uid, data_name):
    bucket = s3.Bucket(BUCKET_NAME)
    objects = [obj.key for obj in bucket.objects.filter(Prefix=uid).all()]
    get_items = [item.split("/")[2] for item in objects if data_name in item]
    get_items.sort(reverse=True)
    return get_items[-1]


def func_create_db_sqlite(uid, data_name):
    DB_URL = f"mysql+mysqlconnector://root@db:3306/{uid}?charset=utf8"
    engine = create_engine(DB_URL, echo=True)

    s3_data = _get_data_s3(uid, data_name)
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=uid + '/upload_data/' + s3_data)
    table_name = data_name.replace(".csv", "").replace(".tsv", "")
    df = pd.read_csv(obj['Body'])
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print("data count :", df.shape[0])
    return f"{table_name} created!!"


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
    def _calc_month(x):
        month = x.month
        if month < 10:
            out = "0" + str(month)
        else:
            out = str(month)
        return out

    def _calc_quarter(x):
        year = x.year
        quarter = (x.month - 1) // 3 + 1
        return str(year) + " " + str(quarter) + "Q"

    df[date_column] = df[date_column].swifter.apply(lambda x: parse(x))
    df["year"] = df[date_column].swifter.apply(lambda x: x.year)
    df["quarter"] = df[date_column].swifter.apply(lambda x: _calc_quarter(x))
    df["month"] = df[date_column].swifter.apply(lambda x: str(x.year) + '-' + _calc_month(x))

    return df


# 集計のためのヘルパー関数
def _func_period_calc(df: DataFrame,
                      period: str,
                      amount_column: str,
                      date_column: str,
                      axis=None):
    def _calc_quarter(x):
        year = x.year
        quarter = (x.month - 1) // 3 + 1
        return str(year) + " " + str(quarter) + "Q"

    def _calc_month(x):
        month = x.month
        if month < 10:
            out = "0" + str(month)
        else:
            out = str(month)
        return out

    if period == 'year':
        df["year"] = df[date_column].swifter.apply(lambda x: x.year)
    elif period == 'quarter':
        df["quarter"] = df[date_column].swifter.apply(lambda x: _calc_quarter(x))
    elif period == 'month':
        df["month"] = df[date_column].swifter.apply(lambda x: str(x.year) + '-' + _calc_month(x))

    if axis is not None:
        calc = df[[period, axis, amount_column]].groupby([period, axis])
    else:
        calc = df[[period, amount_column]].groupby(period)
    return calc


def _calc(df: DataFrame,
          period: str,
          kpi_column: str,
          date_column: str,
          method: str,
          axis=None):
    """
    Defines the process of calculating the KPI; data about the KPI item and the period covered,
    The process receives data related to the KPI item, the period covered, the item name for the date,
    the name of the target KPI item, and the calculation method, and performs the calculation.
    """
    calc = _func_period_calc(df, period, kpi_column, date_column, axis)
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
             date_column,
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

    if df[date_column].dtypes == object:
        df[date_column] = df[date_column].swifter.apply(lambda x: parse(x))
    if (terms_column is not None) & (terms is not None):
        if (type(terms_column) == list) & (type(terms) == list):
            for term_col, term in zip(terms_column, terms):
                df = df[df[term_col] == term]
        else:
            df = df[df[terms_column] == terms]
    output = _calc(df, period, kpi_column, date_column, method, axis)
    output = output.rename(columns={kpi_column: 'output'})
    return output


def func_calc_kpi_top_bottom(df,
                        period,
                        top_column,
                        bottom_column,
                        date_column,
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
    top = func_calc_kpi(df, period, top_column, date_column, method_top, terms_column, terms, axis)
    bottom = func_calc_kpi(df, period, bottom_column, date_column, method_bottom, terms_column, terms, axis)

    top = pd.DataFrame(top)
    bottom = pd.DataFrame(bottom)
    top["output"] = top[top.columns[-1]] / bottom[bottom.columns[-1]]

    if axis is not None:
        return top[[period, axis, "output"]]
    else:
        return top[[period, "output"]]

