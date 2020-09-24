-- 触发器（写入）
DELIMITER //
CREATE trigger metadb.sync_tasks_insert_trigger
AFTER INSERT ON metadb.sync_tasks_model
FOR EACH ROW
begin
  -- 写入ods作业记录
  INSERT INTO metadb.sync_tasks_info
    (
     task_id
     ,dag_id
     ,business
     ,source_platform
     ,target_platform
     ,source_handler
     ,target_handler
     ,source_db
     ,source_table
     ,target_db
     ,target_table
     ,dw_level
     ,sync_level
     ,granularity
     ,inc_column
     ,inc_date_type
     ,inc_date_format
     ,unique_column
     ,no_run_time
     ,fields_terminated
     ,life_cycle
     ,is_history
     ,depends_on_past
     ,yarn_queue
     ,hive_config_parameter
     ,execution_timeout
     ,petitioner
     ,operator
     ,status
     ,comments
     ,create_user
     ,update_user
    )
    VALUES (
    concat_ws('_','ods',NEW.business,NEW.source_db,NEW.source_table),
    NEW.dag_id,
    NEW.business,
    NEW.source_platform,
    'hive',
    NEW.source_handler,
    'hive',
    NEW.source_db,
    NEW.source_table,
    'ods',
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table),
    'ods',
    'ods',
    NEW.granularity,
    NEW.inc_column,
    NEW.inc_date_type,
    NEW.inc_date_format,
    NEW.unique_column,
    NEW.no_run_time,
    '/001' -- fields_terminated,
    ,NEW.life_cycle
	,1 -- NEW.is_history
	,NEW.depends_on_past
    ,NEW.yarn_queue
	,'' -- NEW.hive_config_parameter
    ,NEW.execution_timeout
    ,'' -- NEW.petitioner
	,NEW.operator
	,NEW.status
	,NEW.comments
    ,NEW.create_user
	,NEW.update_user
    );
   -- 写入snap记录
    INSERT INTO metadb.sync_tasks_info
    (
    task_id
     ,dag_id
     ,business
     ,source_platform
     ,target_platform
     ,source_handler
     ,target_handler
     ,source_db
     ,source_table
     ,target_db
     ,target_table
     ,dw_level
     ,sync_level
     ,granularity
     ,inc_column
     ,inc_date_type
     ,inc_date_format
     ,unique_column
     ,no_run_time
     ,fields_terminated
     ,life_cycle
     ,is_history
     ,depends_on_past
     ,yarn_queue
     ,hive_config_parameter
     ,execution_timeout
     ,petitioner
     ,operator
     ,status
     ,comments
     ,create_user
     ,update_user
    )
    VALUES (
    concat_ws('_','snap',NEW.business,NEW.source_db,NEW.source_table), -- task_id
    NEW.dag_id,
    NEW.business,
    'hive', -- source_platform
    'hive', -- target_platform
    'hive', -- source_handler
    'hive', -- target_handler
    'ods',  -- source_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
    'snap', -- target_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
    'ods', -- dw_level
    'snap', -- sync_level
    NEW.granularity
    ,NEW.inc_column
    ,NEW.inc_date_type
    ,NEW.inc_date_format
    ,NEW.unique_column
    ,NEW.no_run_time
    ,'/001' -- NEW.fields_terminated
    ,NEW.life_cycle
	,1 -- NEW.is_history
	,NEW.depends_on_past
    ,NEW.yarn_queue
    ,'' -- NEW.hive_config_parameter
    ,NEW.execution_timeout
    ,'' -- NEW.petitioner
	,NEW.operator
	,NEW.status
	,NEW.comments
    ,NEW.create_user
	,NEW.update_user
    );
	insert into metadb.etl_job_dep
    (task_id,dep_task_id,status)
    select concat_ws('_','snap',NEW.business,NEW.source_db,NEW.source_table),concat_ws('_','ods',NEW.business,NEW.source_db,NEW.source_table),1
    ;
END; //
DELIMITER ;

-- 删除
DELIMITER //
CREATE TRIGGER metadb.sync_tasks_delete_trigger
AFTER DELETE ON metadb.sync_tasks_model
FOR EACH ROW
begin
  -- 删除作业
  delete from metadb.sync_tasks_info
  where task_id in(concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table),concat_ws('_','ods',OLD.business,OLD.source_db,OLD.source_table))
  ;
  -- 删除依赖
  delete from metadb.etl_job_dep where task_id = concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table);
  delete from metadb.etl_job_dep where dep_task_id = concat_ws('_','ods',OLD.business,OLD.source_db,OLD.source_table);
  delete from metadb.etl_job_dep where dep_task_id = concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table);
END; //
DELIMITER ;

-- 更改
DELIMITER //
CREATE TRIGGER metadb.sync_tasks_update_trigger
AFTER UPDATE ON metadb.sync_tasks_model
FOR EACH ROW
begin
   delete from metadb.sync_tasks_info
   where task_id in(concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table),concat_ws('_','ods',OLD.business,OLD.source_db,OLD.source_table))
   ;
   -- 写入ods作业记录
  INSERT INTO metadb.sync_tasks_info
    (
     task_id
     ,dag_id
     ,business
     ,source_platform
     ,target_platform
     ,source_handler
     ,target_handler
     ,source_db
     ,source_table
     ,target_db
     ,target_table
     ,dw_level
     ,sync_level
     ,granularity
     ,inc_column
     ,inc_date_type
     ,inc_date_format
     ,unique_column
     ,no_run_time
     ,fields_terminated
     ,life_cycle
     ,is_history
     ,depends_on_past
     ,yarn_queue
     ,hive_config_parameter
     ,execution_timeout
     ,petitioner
     ,operator
     ,status
     ,comments
     ,create_user
     ,update_user
    )
    VALUES (
    concat_ws('_','ods',NEW.business,NEW.source_db,NEW.source_table),
    NEW.dag_id,
    NEW.business,
    NEW.source_platform,
    'hive',
    NEW.source_handler,
    'hive',
    NEW.source_db,
    NEW.source_table,
    'ods',
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table),
    'ods',
    'ods',
    NEW.granularity,
    NEW.inc_column,
    NEW.inc_date_type,
    NEW.inc_date_format,
    NEW.unique_column,
    NEW.no_run_time,
    '/001' -- fields_terminated,
    ,NEW.life_cycle
	,1 -- NEW.is_history
	,NEW.depends_on_past
    ,NEW.yarn_queue
	,'' -- NEW.hive_config_parameter
    ,NEW.execution_timeout
    ,'' -- NEW.petitioner
	,NEW.operator
	,NEW.status
	,NEW.comments
    ,NEW.create_user
	,NEW.update_user
    );
   -- 写入snap记录
    INSERT INTO metadb.sync_tasks_info
    (
    task_id
     ,dag_id
     ,business
     ,source_platform
     ,target_platform
     ,source_handler
     ,target_handler
     ,source_db
     ,source_table
     ,target_db
     ,target_table
     ,dw_level
     ,sync_level
     ,granularity
     ,inc_column
     ,inc_date_type
     ,inc_date_format
     ,unique_column
     ,no_run_time
     ,fields_terminated
     ,life_cycle
     ,is_history
     ,depends_on_past
     ,yarn_queue
     ,hive_config_parameter
     ,execution_timeout
     ,petitioner
     ,operator
     ,status
     ,comments
     ,create_user
     ,update_user
    )
    VALUES (
    concat_ws('_','snap',NEW.business,NEW.source_db,NEW.source_table), -- task_id
    NEW.dag_id,
    NEW.business,
    'hive', -- source_platform
    'hive', -- target_platform
    'hive', -- source_handler
    'hive', -- target_handler
    'ods',  -- source_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
    'snap', -- target_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
    'ods', -- dw_level
    'snap', -- sync_level
    NEW.granularity
    ,NEW.inc_column
    ,NEW.inc_date_type
    ,NEW.inc_date_format
    ,NEW.unique_column
    ,NEW.no_run_time
    ,'/001' -- NEW.fields_terminated
    ,NEW.life_cycle
	,1 -- NEW.is_history
	,NEW.depends_on_past
    ,NEW.yarn_queue
    ,'' -- NEW.hive_config_parameter
    ,NEW.execution_timeout
    ,'' -- NEW.petitioner
	,NEW.operator
	,NEW.status
	,NEW.comments
    ,NEW.create_user
	,NEW.update_user
    );

   -- 更新依赖
   update metadb.etl_job_dep set task_id = concat_ws('_','snap',NEW.business,NEW.source_db,NEW.source_table) where task_id = concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table);
   update metadb.etl_job_dep set dep_task_id = concat_ws('_','ods',NEW.business,NEW.source_db,NEW.source_table) where dep_task_id = concat_ws('_','ods',OLD.business,OLD.source_db,OLD.source_table);
   update metadb.etl_job_dep set dep_task_id = concat_ws('_','snap',NEW.business,NEW.source_db,NEW.source_table) where dep_task_id = concat_ws('_','snap',OLD.business,OLD.source_db,OLD.source_table);

END; //
DELIMITER ;

