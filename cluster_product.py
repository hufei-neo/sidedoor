from pika_queue import queue
from productname_function import *
from cos_sim import Likelihood
from db_connet import mysql_con


def get_ecom_item_key():
    que = queue()
    l_ecom_item_key = que.connet()
    return l_ecom_item_key


def get_detail():
    mysql_con_ = mysql_con()
    df = mysql_con_.query_data()
    print(df.head(5))
    print(df.info())
    print('-----get query data')
    l_ecom_item_key = get_ecom_item_key()
    df['ecomItemKey'] = df['ecomItemKey'].apply(lambda x: x.encode('utf-8'))
    # l_ecom_item_key = ['1010500000020877','1010500000056223','1010100000006939']
    df_all = df.loc[df['ecomItemKey'].isin(l_ecom_item_key)]
    print('------get not confirm ecom_key')
    print(df_all)
    df_all = df_all.pipe(fill_na).assign(
        name=lambda d: d.apply(
            clear_item_name,
            axis=1)). assign(
        item_name_slug=lambda d: d.apply(
            parse_slug,
            axis=1)).pipe(fill_na). assign(
        name_final=lambda d: d.apply(
            compare_name,
            axis=1))
    print('-----master_name is done')
    return df_all


def cluter(max_master_id):
    likelihood = Likelihood()
    dfnew_3_1 = get_detail()
    dfnew_3_1['updateMasterItemKey'] = dfnew_3_1.apply(
        lambda x: x.index + max_master_id + 20)

    dfnew_3_1['probablity'] = 0.00
    dfnew_3_1['masterCreateDt'] = '20191201'
    dfnew_3_1['updateMasterItemName'] = dfnew_3_1['name_final']
    print('-----cluster is start')
    for i in range(len(dfnew_3_1) - 1):
        for j in range(len(dfnew_3_1) - 1 - i):
            j = j + i + 1
            a = likelihood.likelihood(
                str(dfnew_3_1['name'][i]),
                str(dfnew_3_1['name'][j]),
                punctuation=True)
            if a >= 0.8:
                dfnew_3_1['updateMasterItemKey'][j] = dfnew_3_1['updateMasterItemKey'][i]
                dfnew_3_1['updateMasterItemName'][j] = dfnew_3_1['updateMasterItemName'][i]
                dfnew_3_1['probablity'][j] = a
                break
    print('-----cluster is done')
    return dfnew_3_1[['ecomItemKey',
                      'mstrBrandName',
                      'updateMasterItemKey',
                      'updateMasterItemName',
                      'masterCreateDt',
                      'upcCode']]

# df = cluter(70000)
# print(df)