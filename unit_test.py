from master_web_url import *

# check upc_code相同的是不是在一同一个master_id下


def test_upc(max_master_id):
    df = add_web_url(max_master_id)

    q = """
        SELECT ecom_item_key,item_name,master_item_key,master_item_name,upc_code from df_all order by upc_code;
    """

    data_end = pysqldf(q)

    data_check = data_end.loc[data_end['upcCode'].notna(), :].reset_index(
        drop=True)

    data_check['check_1'] = 0
    for i in range(1, len(data_check)):
        if data_check['upcCode'][i] == data_check['upc_code'][i - 1]:
            if data_check['masterItemKey'][i] != data_check['masterItemKey'][i - 1]:
                data_check['check_1'][i] = 1

    print(data_check['check_1'].value_counts())

# test_upc(70000)
