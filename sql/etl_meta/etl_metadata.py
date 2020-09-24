# -*- coding: utf-8 -*-
# @Time    : 2020/5/26 15:28
# @Author  : wangsong12
# @FileName: etl_metadata.py
# @Software: PyCharm

class EtlMetaDataSQL():
 def __init__(self,sqlName=None):
    self.sql_name = sqlName
    self.sql = None
    self.set_sql()
 def set_sql(self):
######################################定义sql变量begin######################################
  #获取数据库连接句柄
  get_handle_sql = """
    select
         b.host,b.port,b.user_name,b.password,b.db_name
    from metadb.conn_db_handle a
    inner join metadb.conn_db_info b
    on a.handle_conn_code = b.handle_conn_code
    where handle_code = '%s'
  """%("##{handle_code}##")
  #获取创建dags
  get_data_dags_sql = """
    select dag_id
           ,owner
           ,retries
           ,batch_type
           ,schedule_interval
           ,depends_on_past
           ,priority_weight
           ,status
    from metadb.dags_info
    where status = 1
      and exec_type = '%s'
  """%("##{exec_type}##")
  #获取创建采集tasks
  get_data_sync_tasks_sql = """
    select  task_id
            ,dag_id
            ,source_db
            ,source_table
            ,target_db
            ,target_table
            ,source_handler
            ,null resources
            ,granularity
            ,null batch_type
            ,inc_column
            ,null inc_date_type
            ,null inc_date_format
            ,0 num_hour
            ,unique_column
            ,status
            ,sync_level
            ,no_run_time
            ,operator
            ,target_handler
            ,source_platform
            ,target_platform
    from metadb.sync_tasks_info
    where status = 1
      and dag_id = '%s'
  """ % ("##{dag_id}##")

  #删除etl作业重跑每天条数记录表
  delete_etl_job_rows_sql = """
    delete from metadb.etl_job_rows 
    where source_db='%s' and source_table='%s' 
      and exec_date='%s' and module in ('sync_inc','sync_full')
  """ % ("##{source_db}##", "##{source_table}##", "##{exec_date}##")
  #获取来源、目标抽取条数
  get_source_target_data_sql = """
    select source_rows,target_rows
    from metadb.etl_job_rows
    where source_db='%s'
      and source_table='%s'
      and exec_date='%s'
      and module = '%s'
    order by create_time desc
    limit 1
  """ % ("##{source_db}##", "##{source_table}##", "##{exec_date}##", "##{module}##")
  # 获取落地文本条数
  get_file_rows_sql = """
    select target_rows
    from metadb.etl_job_rows
    where target_db = 'file'
      and module = 'sync_inc'
      and exec_date='%s'
      and source_db = '%s'
      and source_table = '%s'
    order by create_time desc limit 1
  """%("##{exec_date}##","##{source_db}##","##{source_table}##")
  #获取同步表字段主键
  get_table_kcolumn_sql = """
    select key_column 
    from metadb.sync_table_kcolumn
    where hive_db='%s' 
      and hive_table='%s' 
    order by update_time 
    desc limit 1
  """%("##{hive_db}##","##{hive_table}##")
  #保存主键字段
  insert_table_kcolumn_sql = """
    insert into metadb.sync_table_kcolumn
    (hive_db,hive_table,key_column)
    select '%s','%s','%s'
    """%("##{hive_db}##","##{hive_table}##","##{key_column}##")
  #删除历史保存主键字段
  delete_table_kcolumn_sql = """
    delete from metadb.sync_table_kcolumn 
    where hive_db='%s' and hive_table='%s' and create_time < '%s'
  """%("##{hive_db}##","##{hive_table}##","##{create_time}##")
  #获取敏感sql
  get_sensitive_sql = """
    select distinct t.`db`, 
           t.`table`, 
           t.`column`,
           ifnull(sensitive_length,0),
           ifnull(exceed_length,0) ,
           sensitive_type,
           sensitive_value,
           sensitive_search_word,
           sensitive_value_type
    from metadb.sensitive_column t 
    where t.`status` = 1
      and t.`db` = '%s'
      and t.`table` = '%s'
      and t.`column` = '%s'
  """%("##{db}##","##{table}##","##{column}##")
  #敏感信息
  get_sensitive_column_sql = "select t.`db`, t.`table`, t.`column` from metadb.sensitive_column t where t.`status` = 1"

  #同步表字段
  insert_sync_columns_info_sql = """
    insert into metadb.sync_columns_info
    (
    source_db,
    source_table,
    target_db,
    target_table,
    column_name,
    column_type,
    column_comment,
    column_key,
    is_alter,
    status
    ) select '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'
  """ % ("##{source_db}##","##{source_table}##","##{target_db}##","##{target_table}##"
                   ,"##{column_name}##","##{column_type}##","##{column_comment}##","##{column_key}##"
                   ,"##{is_alter}##",'##{status}##')

  #查看是否有同步过
  get_is_sync_table_sql = """
      select 1 
      from metadb.sync_columns_info 
      where source_db='%s' 
        and source_table='%s' 
        and target_db='%s' 
        and target_table='%s'
  """ % ("##{source_db}##","##{source_table}##","##{target_db}##","##{target_table}##")

#记录同步条数
  insert_etl_job_rows_sql = """
     insert into metadb.etl_job_rows
     (source_db
      ,source_table
      ,target_db
      ,target_table
      ,module
      ,exec_date
      ,begin_exec_time
      ,end_exec_time
      ,begin_system_time
      ,end_system_time
      ,source_rows
      ,target_rows
     ) VALUES ('%s', '%s',  '%s', '%s','%s', '%s',  '%s', '%s','%s','%s',cast(%s as signed),cast(%s as signed))
  """ % ("##{source_db}##","##{source_table}##","##{target_db}##","##{target_table}##","##{module}##"
           ,"##{exec_date}##","##{begin_exec_time}##","##{end_exec_time}##","##{begin_system_time}##","##{end_system_time}##"
           ,"##{source_rows}##","##{target_rows}##")
  #获取依赖
  get_task_dep_sql = """
      select c.dag_id  as dep_dag_id
          ,c.task_id as dep_task_id
          ,a.task_id
          ,a.dag_id
          ,c.schedule_interval as dep_schedule_interval
      from metadb.v_task_info a
      inner join metadb.etl_job_dep b
      on a.task_id = b.task_id
      and b.status =  1
      inner join metadb.v_task_info c
      on b.dep_task_id = c.task_id
      where b.task_id in('%s')
  """%("##{task_id}##")
  #获取etl任务
  get_etl_task_sql = """     
      select
           check_task_id as task_id
           ,check_dag_id as dag_id
           ,null as business
           ,null as dw_level
           ,null as granularity
           ,check_db as target_db
           ,check_table as target_table
           ,check_column as unique_column
           ,null as no_run_time
           ,null as life_cycle
           ,null as depends_on_past
           ,'spark' as engine_type
           ,null as yarn_queue
           ,null as hive_config_parameter
           ,null as spark_config_parameter
           ,null as spark_num_executors
           ,null as spark_executor_cores
           ,null as spark_executor_memory
           ,null as spark_driver_memory
           ,null as spark_sql_shuffle_partitions
           ,null as execution_timeout
           ,null as petitioner
           ,null as operator
           ,check_filter
           ,check_filter_date
           ,check_custom_sql
           ,status_level
           ,'check' as task_type
           ,check_platform_type
      from metadb.check_table_unique
      where check_state = 1
        and check_dag_id = '%s'
           union all
      select
         task_id
         ,dag_id
         ,business
         ,dw_level
         ,granularity
         ,target_db
         ,target_table
         ,unique_column
         ,no_run_time
         ,life_cycle
         ,depends_on_past
         ,engine_type
         ,yarn_queue
         ,hive_config_parameter
         ,spark_config_parameter
         ,spark_num_executors
         ,spark_executor_cores
         ,spark_executor_memory
         ,spark_driver_memory
         ,spark_sql_shuffle_partitions
         ,execution_timeout
         ,petitioner
         ,operator
         ,null as check_filter
         ,null as check_filter_date
         ,null as check_custom_sql
         ,null as status_level
         ,'etl' as task_type
         ,null as check_platform_type
      from metadb.etl_tasks_info
      where status = 1
        and dag_id = '%s'

  """ % ("##{dag_id}##","##{dag_id}##")
#获取etl任务
  get_all_etl_task_sql = """
      select
           task_id
           ,dag_id
           ,business
           ,dw_level
           ,granularity
           ,target_db
           ,target_table
           ,unique_column
           ,no_run_time
           ,life_cycle
           ,depends_on_past
           ,engine_type
           ,yarn_queue
           ,hive_config_parameter
           ,spark_config_parameter
           ,spark_num_executors
           ,spark_executor_cores
           ,spark_executor_memory
           ,spark_driver_memory
           ,spark_sql_shuffle_partitions
           ,execution_timeout
           ,petitioner
           ,operator
      from metadb.etl_tasks_info
      where status = 1
       -- and dag_id = '%s'
  """
  #写入依赖
  insert_depend_sql = """
     insert into metadb.etl_job_dep
     (task_id,dep_task_id,status)
     select '%s','%s',1
  """%("##{task_id}##","##{dep_task_id}##")
  #查找依赖
  get_depend_sql = """
     select count(1) from metadb.etl_job_dep where task_id = '%s'
     and dep_task_id = '%s'
  """%("##{task_id}##","##{dep_task_id}##")
  #删除多余依赖
  get_delete_depend_sql = """
     delete from metadb.etl_job_dep where task_id = '%s' and dep_task_id = '%s'
  """%("##{task_id}##","##{dep_task_id}##")
  #查找所有依赖
  get_list_depend_sql = """
     select distinct dep_task_id from metadb.etl_job_dep where task_id = '%s'
  """%("##{task_id}##")
  #判断依赖是否在采集任务配置表或ETL任务配置表
  get_is_task_sql = """
     select 1 from metadb.etl_tasks_info where task_id = '%s'
        union all
     select 1 from metadb.sync_tasks_info where task_id = '%s'
  """%("##{task_id}##","##{task_id}##")
  #查找上游依赖
  get_upstream_depend_sql = """
     select * from metadb.etl_job_dep where dep_task_id = '%s'
  """%("##{dep_task_id}##")
  # 查找上游依赖
  get_downstream_depend_sql = """
    select * from metadb.etl_job_dep where task_id = '%s'
   """ % ("##{task_id}##")



######################################定义sql变量end######################################
  #定义返回sql
  names = locals()
  self.sql = names[self.sql_name]

 def get_sql(self):
   return self.sql

def get_sql(sqlName=None):
  return EtlMetaDataSQL(sqlName).get_sql()





