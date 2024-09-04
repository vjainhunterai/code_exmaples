import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine,text

engine= create_engine('mysql+pymysql://admin:Gpohealth!#!@dev-db-test.c969yoyq9cyy.us-east-1.rds.amazonaws.com:3306/stg_tbl')

def readMetadata(l2MetadataTable, l2metadb):
    # reading context table for parameters
    l2_metaquery= f"select * from {l2metadb}.{l2MetadataTable}"
    df_metadata= pd.read_sql_query(l2_metaquery, engine)
    l2_keys_meta = df_metadata.set_index('key')['value'].to_dict()
    return l2_keys_meta

def readL1InvTable(l1InvTable,l1dbname):
    # reading customer name and l1 load date from l1_invoice_detail table
    query1= f"select distinct invhdrNameCustomer as Customer_Name ,load_date as L1_load_date from {l1dbname}.{l1InvTable}"
    df_l1invtable=pd.read_sql_query(query1, engine)
    return df_l1invtable
def read2AuditTable(l2AuditTable,l2dbname):
    # reading customer name and l1 load date from l2_audit table
    query1= f"select distinct Customer_Name,L1_load_date from {l2dbname}.{l2AuditTable}"
    df_l2audittable=pd.read_sql_query(query1, engine)
    return df_l2audittable

def deltaCustDetails(df1,df2):
    # capturing Delta customer name and l1 load date
    merge_df=df1.merge(df2, on=['Customer_Name', 'L1_load_date'], how='left', indicator=True)
    result_df=merge_df[merge_df['_merge']=='left_only'].drop(columns=['_merge'])
    return result_df
def main():
    # Reading prameters from metadata table
    l2MetadataTable = "prod_context_l2_l3_python"
    l2AuditTable = "l2_audit_data_python"
    meta_db_name = "stg_tbl"
    meta_l2_keys= readMetadata(l2MetadataTable,meta_db_name)
    l1InvTable=meta_l2_keys["L1_Inv_Table"]
    l2MetadataTable=meta_l2_keys["L2_Inv_Table"]
    l1db_name= meta_l2_keys["L1_server_DB"]
    l2_auditdb=meta_l2_keys["L2_Audit_Db"]
    query_meta_table= meta_l2_keys["L2_Query_Metadata"]
    ctxArea= meta_l2_keys["L2_ctxarea"]

    # capturing required details for executing meta query
    df1 = readL1InvTable(l1InvTable, l1db_name)
    df2 = read2AuditTable(l2AuditTable, l2_auditdb)
    df = deltaCustDetails(df1, df2)
    query1 = f"SELECT ctxval FROM {meta_db_name}.{query_meta_table} where ctxarea='{ctxArea}' order by ctxkey"
    query2 = pd.read_sql_query(query1, engine)
    query = query2.to_string(index=False, header=False)

    # executing queries one by one
    for index,row in df.iterrows():
        Customer_Name=row['Customer_Name']
        L1_load_date=row['L1_load_date']
        tbase_query=query.format(Customer_Name=Customer_Name, L1_load_date=L1_load_date)
        base_query=text(tbase_query)

        with engine.begin() as connection:
            connection.execute(base_query)



if __name__ == "__main__":
    main()
