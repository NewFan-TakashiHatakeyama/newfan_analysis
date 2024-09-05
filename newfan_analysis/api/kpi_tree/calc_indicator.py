from fastapi import APIRouter
from api.kpi_tree.function import *
router = APIRouter()


@router.post("/db")
async def create_db(uid):
    result = func_create_user_database(uid)
    return result

@router.post("/calc_indicator")
async def create_table(uid, data_name):
    result = func_create_db_sqlite(uid, data_name)
    return result

