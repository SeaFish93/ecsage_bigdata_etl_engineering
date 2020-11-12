import os
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session

etl_md = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
mysql_session = set_db_session(SessionType="mysql", SessionHandler="mysql_media")
source_data_sql = """
                    select  account_id
                   from big_data_mdg.media_advertiser a
                   where media = 203
                    -- and account_id 1678065793343502
                """
ok,all_rows = mysql_session.get_all_rows(source_data_sql)

#print(all_rows)
for data in all_rows:
    #print(data[0])
    os.system("""echo "%s">>/tmp/lll1111llddd.log """%(data[0]))
insert_sql = """
           load data local infile '%s' into table metadb.oe_valid_account_interface fields terminated by ' ' lines terminated by '\\n' (media_type,service_code,account_id)
         """ % ("/tmp/lll1111llddd.log")
#etl_md.execute_sql("""delete from metadb.oe_valid_account_interface where media_type=%s and service_code='%s' """ % (MediaType, ServiceCode))
etl_md.local_file_to_mysql(sql=insert_sql)