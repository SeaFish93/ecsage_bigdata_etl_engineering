# -*- coding: utf-8 -*-
# @Time    : 2019/12/12 18:04
# @Author  : wangsong
# @FileName: 采集创建表及触发器.sql
# @Software: PyCharm
# function info:采集创建表及触发器语句
#20191218 新增实现修改采集配置表task_id项及ETL配置表所有项时,则依赖表task_id dep_task_id项自动更新 by wangsong

create table etl.akulaku_sync_dags_info(
id                  int not null AUTO_INCREMENT COMMENT '自增主键',
dag_id              varchar(32) not null COMMENT 'dag唯一标识',
owner               varchar(64) not null COMMENT 'dag所有者',
batch_type          varchar(30) not null comment '批次频率：【hour|day】',
retries             int(4) DEFAULT 0  not null COMMENT 'dag失败时重试次数',
start_date_interval int  null COMMENT '开始时间偏移量',
start_date_type     varchar(16) COMMENT '开始时间偏移量单位。day, hour, minute, month',
end_date_interval   int COMMENT '结束时间偏移量',
end_date_type       varchar(16) COMMENT '结束时间偏移量单位。day, hour, minute, month',
schedule_interval   varchar(16) not null COMMENT '调度周期，crontab表达式',
depends_on_past     int(2)      not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
priority_weight     int(2)      DEFAULT 1 not null COMMENT 'dag中task的优先级',
queue               varchar(32) COMMENT 'dag提交队列',
pool                varchar(32) COMMENT 'dag运行池',
description         varchar(1024) COMMENT 'dag的描述信息',
status              int(2) DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
comments            varchar(512) COMMENT '备注',
create_user         varchar(32) not null COMMENT '创建者',
update_user         varchar(32) not null COMMENT '最后更新者',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT akulaku_data_etl_dags_PK PRIMARY KEY (id),
  UNIQUE KEY akulaku_data_etl_dags_unique_ind_edag_id (dag_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Dag作业配置表'

create table etl.akulaku_sync_tasks_model(
task_id               varchar(100) not null COMMENT 'task唯一标识，格式：【f|d】_【业务名】_【源库名】_【源表名】,字母则为小写',
dag_id                varchar(50) not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名简称】_【auto】_【dag】',
business              varchar(16)  not null COMMENT '业务名',
source_platform       varchar(30)  not null comment '来源端平台',
source_handler         varchar(32)  not null COMMENT '来源数据服务器连接句柄',
source_db              varchar(32)  not null COMMENT '源数据db',
source_table           varchar(100)  not null COMMENT '源数据tb',
domain                varchar(16)  DEFAULT 'other' not null COMMENT '目标数据域。master，business，analysis',
granularity           char(1) not null COMMENT '抽取粒度：F-全量，D-增量',
batch_type            varchar(30)    comment '批次类型，hour：小时，day：T+1',
inc_column            varchar(30)    comment '增量抽取字段',
unique_column         varchar(65) not null COMMENT '源表唯一键，用于row_number(partition by)。如：uid, line_id',
no_run_time           varchar(30) not null COMMENT '错开高峰时间点，格式：0,1,2,3,4,5,6',
life_cycle            bigint  DEFAULT 0 COMMENT '表生命周期，需要保留数据天数，默认为0，则永久保留',
is_snap               int(2) DEFAULT 1 not null COMMENT '是否生成snap表,0为不生成snap表，则落地sensitive',
is_history            int(2) DEFAULT 0 not null COMMENT '是否生成回溯表,默认0为不生成回溯表',
depends_on_past       int(2) DEFAULT 0 not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
erroe_level           varchar(16)  DEFAULT 'blue' not null COMMENT '错误等级。blue\yellow\orange\red',
execution_timeout     int(8) null COMMENT 'task运行超时时长（分钟）',
description           varchar(1024)  null COMMENT 'task的描述信息',
status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
last_run_date         varchar(30) COMMENT '任务调度截止北京日期，格式：2019-12-03，大于此日期后，任务将变为无效，不在调度，默认为空，则永久调度',
petitioner            varchar(32) not null COMMENT '需求提出方，邮箱@前缀',
operator              varchar(32) not null COMMENT '任务负责人，邮箱@前缀',
comments              varchar(512)null COMMENT '备注',
create_user           varchar(32) not null COMMENT '创建者，邮箱@前缀',
update_user           varchar(32) not null COMMENT '最后更新者，邮箱@前缀',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT akulaku_data_sync_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hive采集作业模板表'


create table etl.akulaku_sync_tasks_info(
id                    int  not null AUTO_INCREMENT COMMENT '自增主键',
task_id               varchar(100) not null COMMENT 'task唯一标识，格式：【f|d】_【目标表】',
dag_id                varchar(50) not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】_【dag】',
business              varchar(16)  not null COMMENT '所属业务',
source_platform       varchar(30)  not null comment '来源端平台',
target_platform       varchar(30)  not null comment '目标端平台',
source_handler         varchar(32)  not null COMMENT '来源数据服务器连接句柄',
target_handler         varchar(32)  not null COMMENT '目标数据服务器连接句柄',
source_db              varchar(32)  not null COMMENT '源数据db',
source_table           varchar(100)  not null COMMENT '源数据tb',
target_db              varchar(32)  not null COMMENT '目标db',
target_table           varchar(100)  not null COMMENT '目标表',
dw_level              varchar(16)  not null COMMENT '任务所属dw层级',
sync_level            varchar(16)  not null COMMENT '同步层级：sensitive|ods|snap|history',
domain                varchar(16)  DEFAULT 'other' not null COMMENT '数据域。master，business，analysis',
resources             int(2) DEFAULT 1 not null COMMENT '需要服务器资源等级；1，2，3',
granularity           char(1) not null COMMENT '抽取粒度：F-全量，D-增量',
batch_type            varchar(30)    comment '批次类型，hour：小时，day：T+1',
inc_column            varchar(30)    comment '增量抽取字段',
inc_date_type         varchar(30) null COMMENT '增量抽取时间类型；【TimeStamp|Date|DateTime】',
inc_date_format       varchar(30) null COMMENT '增量抽取时间格式；0：不带杠，1：横杠，2：斜杠',
unique_column         varchar(65) not null COMMENT '源表唯一键，用于row_number(partition by)。如：uid, line_id',
no_run_time           varchar(30) not null COMMENT '错开高峰时间点，格式：0,1,2,3,4,5,6',
fields_terminated     varchar(30)  DEFAULT '/001'  not null COMMENT 'hive表字段间分隔符；默认/001',
life_cycle            bigint  DEFAULT 0 COMMENT '表生命周期，需要保留数据天数，默认为0，则永久保留',
is_history          int(2) DEFAULT 1 not null COMMENT '是否生成回溯表,默认1为不生成回溯表',
depends_on_past       int(2) not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
erroe_level           varchar(16)  DEFAULT 'blue' not null COMMENT '错误等级。blue\yellow\orange\red',
priority_weight       int(2) DEFAULT 1 not null COMMENT 'task的优先级',
queue                 varchar(32) null COMMENT 'task提交队列',
pool                  varchar(32) null COMMENT 'task运行池',
execution_timeout     int(8) null COMMENT 'task运行超时时长（分钟）',
description           varchar(1024)  null COMMENT 'task的描述信息',
petitioner             varchar(32) not null COMMENT '需求提出方',
operator               varchar(32) not null COMMENT '任务负责人',
status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
comments              varchar(512)null COMMENT '备注',
create_user           varchar(32) not null COMMENT '创建者',
update_user           varchar(32) not null COMMENT '最后更新者',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT akulaku_data_sync_tasks_PK PRIMARY KEY (id),
  UNIQUE KEY akulaku_data_sync_tasks_unique_ind_task_id (task_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='hive采集作业信息表'

-- 触发器（写入）
DELIMITER //
CREATE TRIGGER metadb.sync_tasks_insert_trigger
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
    concat('ods','_',NEW.task_id),
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
    NEW.fields_terminated,
    NEW.life_cycle,NEW.is_history,NEW.depends_on_past,
    NEW.yarn_queue,NEW.hive_config_parameter,
    NEW.execution_timeout,
    NEW.petitioner,NEW.operator,NEW.status,NEW.comments,
    NEW.create_user,NEW.update_user
    );
  if NEW.is_snap != 0 then
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
    concat('snap','_',NEW.task_id), -- task_id
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
    NEW.granularity,
    NEW.inc_column,
    NEW.inc_date_type,
    NEW.inc_date_format,
    NEW.unique_column,
    NEW.no_run_time,
    NEW.fields_terminated,
    NEW.life_cycle,NEW.is_history,NEW.depends_on_past,
    NEW.yarn_queue,
    NEW.hive_config_parameter,
    NEW.execution_timeout,
    NEW.petitioner,NEW.operator,NEW.status,NEW.comments,
    NEW.create_user,NEW.update_user
    );
    insert into metadb.etl_job_dep
    (task_id,dep_task_id,status)
    select concat('snap','_',NEW.task_id),concat('ods','_',NEW.task_id),1
    ;
    if NEW.is_history != 0 then
      -- 写入history
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
      concat('history','_',NEW.task_id), -- task_id
      NEW.dag_id,
      NEW.business,
      'hive', -- source_platform
      'hive', -- target_platform
      'hive', -- source_handler
      'hive', -- target_handler
      'snap',  -- source_db
      concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
      'history', -- target_db
      concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
      'ods', -- dw_level
      'history', -- sync_level
      NEW.granularity,
      NEW.inc_column,
      NEW.inc_date_type,
      NEW.inc_date_format,
      NEW.unique_column,
      NEW.no_run_time,
      NEW.fields_terminated,
      NEW.life_cycle,NEW.is_history,NEW.depends_on_past,
      NEW.yarn_queue,
      NEW.hive_config_parameter,
      NEW.execution_timeout,
      NEW.petitioner,NEW.operator,NEW.status,NEW.comments,
      NEW.create_user,NEW.update_user
      );
      insert into metadb.etl_job_dep
      (task_id,dep_task_id,status)
      select concat('history','_',NEW.task_id),concat('snap','_',NEW.task_id),1
      ;
    end if;
  else
    -- 写入sensitive记录
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
    concat('sensitive','_',NEW.task_id), -- task_id
    NEW.dag_id,
    NEW.business,
    'hive', -- source_platform
    'hive', -- target_platform
    'hive', -- source_handler
    'hive', -- target_handler
    'ods',  -- source_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
    'sensitive_data', -- target_db
    concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
    'ods', -- dw_level
    'sensitive', -- sync_level
    NEW.granularity,
    NEW.inc_column,
    NEW.inc_date_type,
    NEW.inc_date_format,
    NEW.unique_column,
    NEW.no_run_time,NEW.fields_terminated,
    NEW.life_cycle,NEW.is_history,NEW.depends_on_past,
    NEW.yarn_queue,NEW.hive_config_parameter,NEW.execution_timeout,
    NEW.petitioner,NEW.operator,NEW.status,NEW.comments,
    NEW.create_user,NEW.update_user
    );
    insert into metadb.etl_job_dep
     (task_id,dep_task_id,status)
    select concat('sensitive','_',NEW.task_id),concat('ods','_',NEW.task_id),1
      ;
  end if;
END; //
DELIMITER ;

-- 删除
DELIMITER //
CREATE TRIGGER etl.akulaku_sync_tasks_delete_trigger
AFTER DELETE ON etl.akulaku_sync_tasks_model
FOR EACH ROW
begin
if OLD.batch_type != 'hour' then
  delete from etl.akulaku_sync_tasks_info
  where task_id in(concat('ods','_',OLD.task_id),concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id),concat('history','_',OLD.task_id) )
  ;
else
  delete from etl.akulaku_sync_tasks_info
  where task_id in(concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id))
  ;
end if;
END; //
DELIMITER ;

-- 更改
DELIMITER //
CREATE TRIGGER etl.akulaku_sync_tasks_update_trigger
AFTER UPDATE ON etl.akulaku_sync_tasks_model
FOR EACH ROW
begin
-- 更新依赖
if NEW.task_id != OLD.task_id then
   update etl.akulaku_etl_job_dep_new set task_id = concat(case when NEW.is_snap != 1 then 'sensitive' else 'snap' end,'_',NEW.task_id) where task_id = concat(case when OLD.is_snap != 1 then 'ods' else 'snap' end,'_',OLD.task_id);
   update etl.akulaku_etl_job_dep_new set dep_task_id = concat(case when NEW.is_snap != 1 then 'sensitive' else 'snap' end,'_',NEW.task_id) where dep_task_id = concat(case when OLD.is_snap != 1 then 'ods' else 'snap' end,'_',OLD.task_id);
end if;
if OLD.batch_type != 'hour' then
   -- ods
   update etl.akulaku_sync_tasks_info set
   task_id = concat('ods','_',NEW.task_id),
   dag_id = NEW.dag_id,
   business = NEW.business,
   source_platform = NEW.source_platform,
   source_handler = NEW.source_handler,
   source_db = NEW.source_db,
   source_table = NEW.source_table,
   target_table = concat_ws('_',NEW.business,NEW.source_db,NEW.source_table),
   domain = NEW.domain,
   granularity = NEW.granularity,
   batch_type = NEW.batch_type,
   inc_column = NEW.inc_column,
   unique_column = NEW.unique_column,
   no_run_time = NEW.no_run_time,
   life_cycle = NEW.life_cycle,
   is_history = NEW.is_history,
   depends_on_past = NEW.depends_on_past,
   erroe_level = NEW.erroe_level,
   execution_timeout = NEW.execution_timeout,
   description = NEW.description,
   petitioner = NEW.petitioner,
   operator = NEW.operator,
   status = NEW.status,
   comments = NEW.comments,
   create_user = NEW.create_user,
   update_user = NEW.update_user
   where task_id = concat('ods','_',OLD.task_id)
   ;
   if New.is_snap != 1 then
     -- 剔除history
     delete from etl.akulaku_sync_tasks_info where task_id in (concat('history','_',OLD.task_id));
     -- 更新sensitive记录
     update etl.akulaku_sync_tasks_info set
     task_id = concat('sensitive','_',NEW.task_id), -- task_id
     dag_id = NEW.dag_id,
     business = NEW.business,
     source_table = concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
     target_table = concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
     target_db = 'sensitive_data',
     sync_level = 'sensitive',
     domain = NEW.domain,
     granularity = NEW.granularity,
     batch_type = NEW.batch_type,
     inc_column = NEW.inc_column,
     unique_column = NEW.unique_column,
     no_run_time = NEW.no_run_time,
     life_cycle = NEW.life_cycle,
     is_history = NEW.is_history,
     depends_on_past = NEW.depends_on_past,
     erroe_level = NEW.erroe_level,
     execution_timeout = NEW.execution_timeout,
     description = NEW.description,
     petitioner = NEW.petitioner,
     operator = NEW.operator,
     status = NEW.status,
     comments = NEW.comments,
     create_user = NEW.create_user,
     update_user = NEW.update_user
     where task_id in (concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id))
     ;
   else
     -- 更新snap
     update etl.akulaku_sync_tasks_info set
     task_id = concat('snap','_',NEW.task_id), -- task_id
     dag_id = NEW.dag_id,
     business = NEW.business,
     source_table = concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
     target_table = concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
     target_db = 'snap',
     sync_level = 'snap',
     domain = NEW.domain,
     granularity = NEW.granularity,
     batch_type = NEW.batch_type,
     inc_column = NEW.inc_column,
     unique_column = NEW.unique_column,
     no_run_time = NEW.no_run_time,
     life_cycle = NEW.life_cycle,
     is_history = NEW.is_history,
     depends_on_past = NEW.depends_on_past,
     erroe_level = NEW.erroe_level,
     execution_timeout = NEW.execution_timeout,
     description = NEW.description,
     petitioner = NEW.petitioner,
     operator = NEW.operator,
     status = NEW.status,
     comments = NEW.comments,
     create_user = NEW.create_user,
     update_user = NEW.update_user
     where task_id in (concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id))
     ;
     if NEW.is_history != 0 then
        -- 剔除history
        delete from etl.akulaku_sync_tasks_info where task_id = concat('history','_',OLD.task_id);
        -- 写入history
        INSERT INTO etl.akulaku_sync_tasks_info
        (
        task_id,dag_id,business,source_platform,target_platform,
        source_handler,target_handler,source_db,source_table,
        target_db,target_table,dw_level,sync_level,domain,
        resources,granularity,batch_type,inc_column,unique_column,no_run_time,life_cycle,is_history,depends_on_past,
        erroe_level,priority_weight,queue,pool,execution_timeout,
        description,petitioner,operator,status,comments,
        create_user,update_user
        )
        VALUES (
        concat('history','_',NEW.task_id), -- task_id
        NEW.dag_id,
        NEW.business,
        'hive', -- source_platform
        'hive', -- target_platform
        'hive', -- source_handler
        'hive', -- target_handler
        'snap',  -- source_db
        concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- source_table
        'history', -- target_db
        concat_ws('_',NEW.business,NEW.source_db,NEW.source_table), -- target_table
        'ods', -- dw_level
        'history', -- sync_level
        NEW.domain,
        1,     -- resources
        NEW.granularity,
        NEW.batch_type,
        NEW.inc_column,
        NEW.unique_column,
        NEW.no_run_time,
        NEW.life_cycle,NEW.is_history,NEW.depends_on_past,
        NEW.erroe_level,1,null,null,NEW.execution_timeout,
        NEW.description,NEW.petitioner,NEW.operator,NEW.status,NEW.comments,
        NEW.create_user,NEW.update_user
        );
     else
        -- 剔除history
        delete from etl.akulaku_sync_tasks_info where task_id = concat('history','_',OLD.task_id);
    end if;
   end if;
else
  if New.is_snap != 1 then
     -- 更新sensitive记录
     update etl.akulaku_sync_tasks_info set
     task_id = concat('sensitive','_',NEW.task_id), -- task_id
     dag_id = NEW.dag_id,
     business = NEW.business,
     source_table = NEW.source_table, -- source_table
     target_table = NEW.source_table, -- target_table
     target_db = 'sensitive_data',
     sync_level = 'sensitive',
     domain = NEW.domain,
     granularity = NEW.granularity,
     batch_type = NEW.batch_type,
     inc_column = NEW.inc_column,
     unique_column = NEW.unique_column,
     no_run_time = NEW.no_run_time,
     life_cycle = NEW.life_cycle,
     is_history = NEW.is_history,
     depends_on_past = NEW.depends_on_past,
     erroe_level = NEW.erroe_level,
     execution_timeout = NEW.execution_timeout,
     description = NEW.description,
     petitioner = NEW.petitioner,
     operator = NEW.operator,
     status = NEW.status,
     comments = NEW.comments,
     create_user = NEW.create_user,
     update_user = NEW.update_user
     where task_id in (concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id))
     ;
   else
     -- 更新snap
     update etl.akulaku_sync_tasks_info set
     task_id = concat('snap','_',NEW.task_id), -- task_id
     dag_id = NEW.dag_id,
     business = NEW.business,
     source_table = NEW.source_table, -- source_table
     target_table = NEW.source_table, -- target_table
     target_db = 'rt_snap',
     sync_level = 'snap',
     domain = NEW.domain,
     granularity = NEW.granularity,
     batch_type = NEW.batch_type,
     inc_column = NEW.inc_column,
     unique_column = NEW.unique_column,
     no_run_time = NEW.no_run_time,
     life_cycle = NEW.life_cycle,
     is_history = NEW.is_history,
     depends_on_past = NEW.depends_on_past,
     erroe_level = NEW.erroe_level,
     execution_timeout = NEW.execution_timeout,
     description = NEW.description,
     petitioner = NEW.petitioner,
     operator = NEW.operator,
     status = NEW.status,
     comments = NEW.comments,
     create_user = NEW.create_user,
     update_user = NEW.update_user
     where task_id in (concat('snap','_',OLD.task_id),concat('sensitive','_',OLD.task_id))
     ;
    end if;
end if;
END; //
DELIMITER ;


-- 更改etl.akulaku_etl_job_new
DELIMITER //
CREATE TRIGGER etl.akulaku_etl_job_new_update_trigger
AFTER UPDATE ON etl.akulaku_etl_job_new
FOR EACH ROW
begin

-- 更新依赖
if NEW.target_db != OLD.target_db or NEW.extract_particle_size != OLD.extract_particle_size or NEW.source != OLD.source or NEW.source_db != OLD.source_db or NEW.source_table != OLD.source_table or NEW.target_table != OLD.target_table or NEW.dw_level != OLD.dw_level then
   update etl.akulaku_etl_job_dep_new set task_id = concat(case when NEW.dw_level = 'ods' and NEW.target_db='snap' then 'snap'
                                                                when NEW.dw_level = 'ods' and NEW.target_db='ods' then 'sensitive'
                                                                else NEW.dw_level
                                                           end,
                                                           '_',
                                                           case when LOWER(NEW.dw_level) = 'ods' then concat(LOWER(NEW.extract_particle_size),'_',NEW.source,'_',NEW.source_db,'_',NEW.source_table)
                                                                else NEW.target_table
                                                           end
                                                          )
   where task_id = concat(case when OLD.dw_level = 'ods' and OLD.target_db='snap' then 'snap'
                               when OLD.dw_level = 'ods' and OLD.target_db='ods' then 'sensitive'
                               else OLD.dw_level
                          end,
                          '_',
                          case when LOWER(OLD.dw_level) = 'ods' then concat(LOWER(OLD.extract_particle_size),'_',OLD.source,'_',OLD.source_db,'_',OLD.source_table)
                               else OLD.target_table
                          end
                        );
   update etl.akulaku_etl_job_dep_new set dep_task_id = concat(case when NEW.dw_level = 'ods' and NEW.target_db='snap' then 'snap'
                                                                    when NEW.dw_level = 'ods' and NEW.target_db='ods' then 'sensitive'
                                                                    else NEW.dw_level
                                                               end,
                                                               '_',
                                                               case when LOWER(NEW.dw_level) = 'ods' then concat(LOWER(NEW.extract_particle_size),'_',NEW.source,'_',NEW.source_db,'_',NEW.source_table)
                                                                    else NEW.target_table
                                                               end
                                                              )
   where dep_task_id = concat(case when OLD.dw_level = 'ods' and OLD.target_db='snap' then 'snap'
                               when OLD.dw_level = 'ods' and OLD.target_db='ods' then 'sensitive'
                               else OLD.dw_level
                          end,
                          '_',
                          case when LOWER(OLD.dw_level) = 'ods' then concat(LOWER(OLD.extract_particle_size),'_',OLD.source,'_',OLD.source_db,'_',OLD.source_table)
                               else OLD.target_table
                          end
                        );
end if;
END; //
DELIMITER ;