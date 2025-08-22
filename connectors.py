# Placeholder for Auxiliary Scrupts
import sys
from pathlib import Path
sys.path.extend([str(Path(__file__).resolve().parent.parent)])
from datetime import datetime

import pymysql
import pandas as pd
import closed_data_m as closed_data
from closed_data_m import mysql
from sqlalchemy import create_engine
from sqlalchemy import update
from uuid import uuid4
from sqlalchemy import MetaData, Table, insert, select, func
import traceback



def data_fetcher(sql_query, *args):
    conn = pymysql.connect(
        host=closed_data.mysql["hostname"],
        user=closed_data.mysql["user"],
        password=closed_data.mysql["password"],
        port=closed_data.mysql["port"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    cursor = conn.cursor()
    if len(args) == 1:
        column = args[-1]
        args = tuple()
        data_dict = {i: 0 for i in column}
    else:
        column = args[-1]
        args = args[:-1]
        data_dict = {i: (0 if i != "Date" else args[0]) for i in column}

    cursor.execute(sql_query, args)
    data = cursor.fetchall()

    df = pd.DataFrame(data)

    if len(df) == 0:
        # df = pd.DataFrame({'Date': [args[1]] , 'Value': [0]})
        df = pd.DataFrame(data_dict, index=[0])

    cursor.close()
    conn.close()

    return df


data_write_engine = f'mysql+mysqldb://{mysql["user"]}:{mysql["password"]}@{mysql["hostname"]}:{mysql["port"]}/{mysql["target_database"]}?charset=utf8mb4'


def write_data_mysql(df, table_name, replace_val = "append"):
    try:
        engine = create_engine(data_write_engine)
        with engine.connect() as conn:
            session = conn.begin()
            try:
                df.to_sql(
                    name=table_name, con=conn, if_exists=replace_val, index=False
                )
                session.commit()
            except Exception as e1:
                session.rollback()
    except Exception as e:
        print(f'error writing data to {table_name}: {e}')
        pass


def update_column(tb_name, dets ):
    metadata = MetaData()
    eng=create_engine(data_write_engine)
    with eng.connect() as conn:
        table = Table(tb_name, metadata, autoload_with=eng)
        time = datetime.now()

        if len(dets) == 3:
            name, ip, ip_id = dets
            stmt = (
                update(table)
                .where(table.c.name == name)
                .values(ip=ip, ip_id=ip_id, created_at=time)
            )
            with eng.connect() as conn:
                stmt_count = select(func.count()).select_from(table)
                total_rows = conn.execute(stmt_count).scalar()

                result = conn.execute(stmt)
                if result.rowcount<=0:
                    stmt_insert = (
                        insert(table)
                        .values(id=total_rows,name=name, ip=ip, ip_id=ip_id, created_at=time)
                    )
                    conn.execute(stmt_insert)
        elif len(dets) == 1:
            ip_id = dets[0]
            stmt = (
                update(table)
                .where(table.c.id == 0)
                .values(ip_id=ip_id, created_at=time)
            )
            with eng.connect() as conn:
                conn.execute(stmt)
