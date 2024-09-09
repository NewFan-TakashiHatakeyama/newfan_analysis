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