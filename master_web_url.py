from pandasql import sqldf
from cluster_product import *
# from pyhive_con import *
from db_connet import *

def query_web_url():
    mysql_con_ = mysql_con()
    front = mysql_con_.query_web_data()
    return front


def pysqldf(q): return sqldf(q, globals())


def add_web_url(max_master_id):
    data_end = query_web_url()
    df = cluter(max_master_id)
    q_2 = """
    select t1.*,t2.web_url,t2.front_possibility from
    df t1 left join
    data_end t2
    on t1.ecomItemKey=t2.ecom_item_key
    order by t1.masterItemKey asc,t2.front_possibility desc
    """
    df_fin = pysqldf(q_2)
    for i in range(1, len(df_fin)):
        if df_fin['master_item_key'][i] == df_fin['master_item_key'][i - 1]:
            df_fin['web_url'][i] = df_fin['web_url'][i - 1]
    df_fin = df_fin.rename(columns={'web_url': 'masterWebUrl'})
    return df_fin
