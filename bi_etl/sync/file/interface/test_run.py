from ecsage_bigdata_etl_engineering.bi_etl.sync.file.interface.remote_proc import exec_remote_proc
from ecsage_bigdata_etl_engineering.common.session.db_session import set_db_session



etl_md = set_db_session(SessionType="mysql", SessionHandler="etl_metadb")
ok,host_data = etl_md.get_all_rows("""select ip,user_name,passwd from metadb.request_account_host limit 1""")
shell_cmd = """
              python3 /root/bigdata_item_code/ecsage_bigdata_etl_engineering/bi_etl/sync/file/interface/run.py > /root/wangsong/t1111t-hnhd-02.log
            """
exec_remote_proc(HostName=host_data[0][0], UserName=host_data[0][1], PassWord=host_data[0][2],
                 ShellCommd=shell_cmd)

