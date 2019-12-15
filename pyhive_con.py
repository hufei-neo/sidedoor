from pyhive import hive
import pandas as pd

class pyhive_db(object):
    def __init__(self):
        self.conn = hive.Connection(
            host='121.46.231.170',
            port=10000,
            auth="CUSTOM",
            database='gnpd_dwh',
            username='developer',
            password='SjOw[+Fmgur^X#1PHbygbQ#m^DnZoXUY')

    def query_data(self, sql):
        """
        Query the data form Apache Drill.
        :return: DataFrame
        """
        con = self.conn
        cursor = con.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        result = pd.DataFrame(result)
        cursor.close()
        con.close()
        return result

# sql = """select a.ecom_item_key,b.web_url,b.front_possibility from gnpd_dwh.h_ecom_item a
# left join (select * from gnpd.product_screenshot_local where pic_tag='front' and web_url is not null)b
# on a.item_crawl_key = b.sku_key
# and a.item_sku_type = b.sku_type"""
# db = pyhive_db()
# front = db.query_data(sql)