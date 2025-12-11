import pyodbc
from sqlalchemy import create_engine
import sqlalchemy.exc
from urllib import parse
import pandas as pd

# Define Database Connection

CONNAS400 = """
Driver={iSeries Access ODBC Driver};
system=10.143.12.10;
Server=AS400;
Database=PROD;
UID=SMY;
PWD=SMY;
"""
# SQLAlchemy connection
server = 'tn-sql'
database = 'autodata'
driver = 'ODBC+Driver+17+for+SQL+Server'
user = 'production'
pwd = parse.quote_plus("Auto@matics")
port = '1433'
database_conn = f'mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver={driver}'
# Make Connection
engine = create_engine(database_conn)
conn = engine.connect()

def get_inv():
    """Get Spare Inventory Data From iSeries AS400"""
    dbcnxn = pyodbc.connect(CONNAS400)
    cursor = dbcnxn.cursor()

    strsql = """SELECT PROD.FPSPRMAST1.SPH_PART,
                       STRIP(PROD.FPSPRMAST1.SPH_ENGPRT),
                       STRIP(PROD.FPSPRMAST1.SPH_DESC1),
                       STRIP(PROD.FPSPRMAST1.SPH_DESC2),
                       STRIP(PROD.FPSPRMAST1.SPH_MFG),
                       STRIP(PROD.FPSPRMAST1.SPH_MFGPRT),
                       STRIP(PROD.FPSPRMAST2.SPD_CABINT),
                       STRIP(PROD.FPSPRMAST2.SPD_DRAWER),
                       PROD.FPSPRMAST2.SPD_QOHCUR,
                       PROD.FPSPRMAST1.SPH_CURSTD,
                       STRIP(PROD.FPSPRMAST2.SPD_REODTE),
                       STRIP(PROD.FPSPRMAST2.SPD_USECC),
                       STRIP(PROD.FPSPRMAST2.SPD_PURCC),
                       STRIP(PROD.FPSPRMAST2.SPD_QREORD)
                FROM PROD.FPSPRMAST1 INNER JOIN PROD.FPSPRMAST2 ON PROD.FPSPRMAST1.SPH_PART = PROD.FPSPRMAST2.SPD_PART
                WHERE (((PROD.FPSPRMAST2.SPD_FACIL)=9))"""
    try:
        cursor.execute(strsql)
        result = cursor.fetchall()
    except Exception as e:
        msg = 'AS400 Inventory Query Failed: ' + str(e)
        result = []
        print(msg)
        print(strsql)
    else:
        msg = str(len(result)) + " AS400 Inventory Records Processed From Inventory Tables"
        print(msg)
    dbcnxn.close()
    return result

def find_new_obs(result_spares):
    """Find any newly obsoleted spare parts"""
    data_type_dict = {'StandardCost': float, 'OnHand': int, 'PartNum': str, 'ReOrderPt': int, 'ReOrderDate': int, 'Cabinet': str, 'Drawer': str}
    df_spares = pd.DataFrame.from_records(result_spares)
    df_spares.columns = ['PartNum', 'EngPartNum', 'Desc1', 'Desc2', 'Mfg', 'MfgPn', 'Cabinet', 'Drawer', 'OnHand',
                         'StandardCost', 'ReOrderDate', 'DeptUse', 'DeptPurch', 'ReOrderPt']
    df_spares = df_spares.dropna()
    df_spares = df_spares.astype(data_type_dict)
    df_spares["Cabinet"] = df_spares["Cabinet"].astype(str)
    df_spares = df_spares.convert_dtypes()

    df_obs = df_spares["Cabinet"].str.contains("OBS", case=False, na=False)

    filtered_df = df_spares[df_obs]
    filtered_df = filtered_df[["PartNum", "EngPartNum"]]
    # print(filtered_df.head())
    return filtered_df

def main():
    test_data = {
        'PartNum': ['123456', '65432'],
        'EngPartNum': ['abcdef', 'zxcvb']
    }
    sql_dtype = {
        "PartNum": sqlalchemy.types.VARCHAR(length=255),
        "EngPartNum": sqlalchemy.types.VARCHAR(length=255),
    }

    test_df = pd.DataFrame(test_data)
    print(test_df)

    test_df.to_sql(name="tblObsSpares", con=engine, schema='eng', if_exists='replace', index=False, dtype=sql_dtype)
    return

if __name__ == '__main__':
    main()