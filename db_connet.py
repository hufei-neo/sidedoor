from sqlalchemy import create_engine
import pandas as pd


class mysql_con():

    def __init__(self):
        self.engine = create_engine(
            'mysql://root:infopower@121.46.231.176:3306/arcade',
            encoding='utf-8')

    def query_data(self):
        engine = self.engine
        columns = ['masterItemKey',
                   'masterItemName',
                   'ecomItemKey',
                   'masterCreateDt',
                   'type',
                   'batch',
                   'mstrBrandName',
                   'company',
                   'ecombrand',
                   'primIngrDosage',
                   'dosageUnit',
                   'weightImperial',
                   'weightImperialUnit',
                   'weightMetric',
                   'weightMetricUnit',
                   'pkgQty',
                   'pgkQtyUnit',
                   'pkgQtyUnitStd',
                   'format',
                   'formatStd',
                   'flavourFragrance',
                   'ecomItemUrl',
                   'ecomSiteId',
                   'ecomItemName',
                   'picUrlJson',
                   'upcCode',
                   'masterWebUrl',
                   'pro']
        df = engine.execute(
            """SELECT * FROM excel_data_sample;""").fetchall()

        df = pd.DataFrame(df, columns=columns)
        return df

    def query_web_data(self):
        engine = self.engine
        columns = ['ecom_item_key', 'web_url', 'front_possibility']
        df = engine.execute(
            """SELECT * FROM ds_ecom_web_url;""").fetchall()

        df = pd.DataFrame(df, columns=columns)
        return df

# mysql_con = mysql_con()
# df = mysql_con.query_data()
