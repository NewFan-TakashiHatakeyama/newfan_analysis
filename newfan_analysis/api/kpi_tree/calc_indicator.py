import requests
import json
from fastapi import APIRouter
from api.kpi_tree.function import *
router = APIRouter()


@router.post("/db")
async def create_db(uid):
    result = func_create_user_database(uid)
    return result

@router.post("/table")
async def create_table(uid, table):
    result = func_create_table(uid, table)
    return result

@router.post("/calc_indicator")
async def calc_indicator_kpi(uid, date_column, user_id_column, indicator_dic_list:list[dict[str, str]]):
    result = func_calc_indicator_kpi(uid, date_column, user_id_column, indicator_dic_list)
    return result

@router.get("/check_mysql")
async def get_mysql(uid):
    result = func_get_mysql(uid)
    return result


@router.get("/bubble_data")
async def get_bubble_data():
    url = "https://fast-53958.bubbleapps.io/version-test/api/1.1/wf/read"
    headers = {'Authorization': 'Bearer f847fa872af3b1d636b41242257cda5f'}
    res_wf = requests.get(url, headers=headers)
    d = json.loads(res_wf.text)
    return d

