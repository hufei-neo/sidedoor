from neo4j import GraphDatabase as G
from master_web_url import *

# docs at https://neo4j.com/docs/api/python-driver/1.7/driver.html
driver = G.driver("bolt://121.46.231.179:7687",
                  auth=("neo4j", "eddc168168"),
                  encrypted=False)

df = add_web_url(70000)
df['ecomItemKey']=df['ecomItemKey'].apply(lambda x: eval(x).decode('utf-8'))
df['masterCreateDt'] = df['masterCreateDt'].astype(str)
df['updateMasterItemKey'] = df['updateMasterItemKey'].astype(str)

# print message
# def print_message(tx):
#     record = tx.run("""MATCH (n:MasterBrand)
#      RETURN n.mstrBrandName""")
#     df = pd.DataFrame(record, columns=['mstrBrandName'])
#     return df
#
#
# with driver.session() as session:
#     df = session.read_transaction(print_message)
# print(df)

# insert massage


def create_node(
        tx,
        mstrBrandName,
        masterItemName,
        masterItemKey,
        masterCreateDt):
    return tx.run("""CREATE (a:MasterItem {mstrBrandName:$mstrBrandName,
                  masterItemName:$masterItemName,
                  pro:'1',
                  masterItemKey:$masterItemKey,
                  masterCreateDt:$masterCreateDt,
                  updateMotel:'1'})""", mstrBrandName=mstrBrandName,
                  masterItemName=masterItemName,
                  masterItemKey=masterItemKey,
                  masterCreateDt=masterCreateDt)


with driver.session() as session:
    df_master = df.drop_duplicates(
        subset=['mstrBrandName',
                'updateMasterItemName',
                'updateMasterItemKey',
                'masterCreateDt']).reset_index(drop=True)
    for i in range(len(df_master)):
        session.write_transaction(create_node,
                                  df_master.loc[i,
                                                 'mstrBrandName'],
                                  df_master.loc[i,
                                                 'updateMasterItemName'],
                                  df_master.loc[i,
                                                 'updateMasterItemKey'],
                                  df_master.loc[i,'masterCreateDt'])

# 给子节点增加一个updateMasterItemKey属性


def set_itemname(tx, ecomItemKey, updateMasterItemKey):
    record = tx.run("""MATCH (n:Item) where n.ecomItemKey=$ecomItemKey
    set n.updateMasterItemKey=$updateMasterItemKey RETURN n""", ecomItemKey=ecomItemKey,
                  updateMasterItemKey=updateMasterItemKey)
    print(record)


with driver.session() as session:
    for i in range(len(df)):
        # session.read_transaction(set_itemname, '1010500000045647', '16705')
        session.read_transaction(set_itemname, df.loc[i, 'ecomItemKey'], df.loc[i, 'updateMasterItemKey'])

# 创建关系
# with driver.session() as session:
#     session.run("""MATCH (n:Item),(m:MasterItem) where
#          n.updateMasterItemKey=m.masterItemKey
#      create (n:Item)-[r:PREDICTED_IS]->(m:MasterItem)""")
#
#     session.run("""MATCH (n:MasterItem),(m:MasterBrand)
#        where  n.mstrBrandName=m.mstrBrandName
#      create (n:MasterItem)-[r:MADE_BY]->(m:MasterBrand)""")
