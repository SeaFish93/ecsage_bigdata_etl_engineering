create table metadb.conn_db_handle
(
 id   int not null AUTO_INCREMENT COMMENT '自增主键',
 handle_code  varchar(100) COMMENT '句柄编码',
 handle_name  varchar(500) COMMENT '句柄名称',
 handle_conn_code  varchar(100) COMMENT '句柄连接池',
 CONSTRAINT conn_db_handle_PK PRIMARY KEY (id),
 UNIQUE KEY conn_db_handle_handle_code (handle_code,handle_conn_code)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='句柄配置表'
;
insert into metadb.conn_db_handle
select
 2,'hive' handle_code
  ,'hive连接' handle_name
   ,'conn_hive' handle_conn_code
from dual
;
create table metadb.conn_db_info_bak
(
 id                int not null AUTO_INCREMENT COMMENT '自增主键',
 handle_conn_code  varchar(100) COMMENT '句柄连接池编码',
 handle_conn_name  varchar(500) COMMENT '句柄连接池名称',
 handle_type  varchar(500) COMMENT '句柄连接池类型',
 host         varchar(3000) COMMENT '数据库ip',
 port         int          COMMENT '数据库端口',
 user_name    varchar(500) COMMENT '数据库用户名',
 password     varchar(500) COMMENT '数据库加密后密码',
 db_name      varchar(500) COMMENT '数据库实例',
 remarks      varchar(500) COMMENT '备注',
 CONSTRAINT conn_db_handle_PK PRIMARY KEY (id),
 UNIQUE KEY conn_db_handle_handle_conn_code (handle_conn_code)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='连接数据库配置表'
;

insert into metadb.conn_db_info
(
  handle_conn_code
  ,handle_conn_name
  ,handle_type
  ,host
  ,port
  ,user_name
  ,password
  ,db_name
 )
 select 'conn_mysql_big_data_mdg','子账户数据连接池','mysql','192.168.30.186',3306,'mdgopr','dWpoZUA5MyNvQDhl','big_data_mdg'
 ;

 insert into metadb.dags_info
(dag_id,exec_type,owner,batch_type,retries,schedule_interval,priority_weight,status)
select 'day_tc_etl_dw','etl','etl','day',3,'30 20 * * *',1,1
;

create table metadb.dags_info(
id                  int not null AUTO_INCREMENT COMMENT '自增主键',
dag_id              varchar(32) not null COMMENT 'dag唯一标识',
exec_type           varchar(32) not null COMMENT '模块：etl每天跑批sql，load每天同步mysql',
owner               varchar(64) not null COMMENT 'dag所有者',
batch_type          varchar(30) not null comment '批次频率：【hour|day】',
retries             int(4) DEFAULT 0  not null COMMENT 'dag失败时重试次数',
schedule_interval   varchar(16) not null COMMENT '调度周期，crontab表达式',
depends_on_past     int(2)  default 1    not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
priority_weight     int(2)      DEFAULT 1 not null COMMENT 'dag中task的优先级',
queue               varchar(32) COMMENT 'dag提交队列',
pool                varchar(32) COMMENT 'dag运行池',
status              int(2) DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
comments            varchar(512) COMMENT '备注',
create_user         varchar(32)  COMMENT '创建者',
update_user         varchar(32)   COMMENT '最后更新者',
create_time         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT sync_dags_info_dags_PK PRIMARY KEY (id),
  UNIQUE KEY sync_dags_info_unique_ind_edag_id (dag_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='Dag作业配置表'
;
create table metadb.sync_tasks_model(
task_id               varchar(100) not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写',
dag_id                varchar(50) not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】',
business              varchar(16)  not null COMMENT '业务名',
source_platform       varchar(30)  not null comment '来源端平台',
source_handler         varchar(32)  not null COMMENT '来源数据服务器连接句柄',
source_db              varchar(32)  not null COMMENT '源数据db',
source_table           varchar(100)  not null COMMENT '源数据tb',
granularity           char(1) not null COMMENT '抽取粒度：F-全量，D-增量',
inc_column            varchar(30)    comment '增量抽取字段',
inc_date_type         varchar(30) null COMMENT '增量抽取时间类型；【TimeStamp|Date|DateTime】',
inc_date_format       varchar(30) null COMMENT '增量抽取时间格式；0：不带杠，1：横杠，2：斜杠',
unique_column         varchar(65)   COMMENT '源表唯一键，用于row_number(partition by)。如：uid, line_id',
no_run_time           varchar(30)   COMMENT '错开高峰时间点，格式：0,1,2,3,4,5,6',
life_cycle            bigint  DEFAULT 0 COMMENT '表生命周期，需要保留数据天数，默认为0，则永久保留',
depends_on_past       int(2) DEFAULT 1 not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
yarn_queue            varchar(32) null COMMENT 'task提交到yarn队列',
execution_timeout     int(8) null COMMENT 'task运行超时时长（分钟）',
description           varchar(1024)  null COMMENT 'task的描述信息',
status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
operator              varchar(32)   COMMENT '任务负责人，邮箱@前缀',
comments              varchar(512)  COMMENT '备注',
create_user           varchar(32)   COMMENT '创建者，邮箱@前缀',
update_user           varchar(32)   COMMENT '最后更新者，邮箱@前缀',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT sync_tasks_model_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hive采集作业模板表'
;

create table metadb.sync_tasks_info(
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
granularity           char(1) not null COMMENT '抽取粒度：F-全量，D-增量',
inc_column            varchar(30)    comment '增量抽取字段',
inc_date_type         varchar(30) null COMMENT '增量抽取时间类型；【TimeStamp|Date|DateTime】',
inc_date_format       varchar(30) null COMMENT '增量抽取时间格式；0：不带杠，1：横杠，2：斜杠',
unique_column         varchar(65) not null COMMENT '源表唯一键，用于row_number(partition by)。如：uid, line_id',
no_run_time           varchar(30) not null COMMENT '错开高峰时间点，格式：0,1,2,3,4,5,6',
fields_terminated     varchar(30)  DEFAULT '/001'  not null COMMENT 'hive表字段间分隔符；默认/001',
life_cycle            bigint  DEFAULT 0 COMMENT '表生命周期，需要保留数据天数，默认为0，则永久保留',
is_history            int(2) DEFAULT 1 not null COMMENT '是否生成回溯表,默认1为不生成回溯表',
depends_on_past       int(2) not null COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
yarn_queue            varchar(32) null COMMENT 'task提交到yarn队列',
hive_config_parameter varchar(500) null COMMENT 'hive配置参数',
execution_timeout     int(8) null COMMENT 'task运行超时时长（分钟）',
petitioner             varchar(32) not null COMMENT '需求提出方',
operator               varchar(32) not null COMMENT '任务负责人',
status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
comments              varchar(512)null COMMENT '备注',
create_user           varchar(32) not null COMMENT '创建者',
update_user           varchar(32) not null COMMENT '最后更新者',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT sync_tasks_info_tasks_PK PRIMARY KEY (id),
UNIQUE KEY sync_tasks_info_unique_ind_task_id (task_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='hive采集作业信息表'
;

insert into metadb.sync_tasks_info
(
task_id
,dag_id
,source_db
,source_table
,target_db
,target_table
,source_handler
,granularity
,inc_column
,unique_column
,status
,sync_level
,no_run_time
,operator
,target_handler
,source_platform
,target_platform
)
select
'd_etl_metadb_sync_tasks_info' task_id
,'day_sync_etl_metadb'  dag_id
,'' source_db
,'' source_table
,'dw' target_db
,'test' target_table
,'' source_handler
,'F' granularity
,null inc_column
,null unique_column
,1 status
,'ods' sync_level
,null no_run_time
,null operator
,'hive' target_handler
,'mysql' source_platform
,'hive' target_platform


create table metadb.etl_job_rows
(
id                    int  not null AUTO_INCREMENT COMMENT '自增主键'
,source_db             varchar(35) COMMENT'来源db'
,source_table          varchar(50) COMMENT'来源表'
,target_db             varchar(35) COMMENT'目标db'
,target_table          varchar(50) COMMENT'目标表'
,module                varchar(35) COMMENT'增量全量类型'
,exec_date             varchar(35) COMMENT'执行日期'
,begin_exec_time       varchar(35) COMMENT'执行开始时间'
,end_exec_time         varchar(35) COMMENT'执行结束时间'
,begin_system_time     varchar(35) COMMENT'系统开始时间'
,end_system_time       varchar(35) COMMENT'系统结束时间'
,source_rows           int         COMMENT'来源条数'
,target_rows           int         COMMENT'目标条数'
,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT sync_tasks_info_tasks_PK PRIMARY KEY (id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='etl作业每天条数记录表'
;
create table metadb.sync_table_kcolumn
(
id                    int  not null AUTO_INCREMENT COMMENT '自增主键'
,hive_db               varchar(32) COMMENT'hive数据库'
,hive_table            varchar(50) COMMENT'hive表名'
,key_column            varchar(50) COMMENT'同步主键字段'
,create_time          datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time          datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT sync_tasks_info_tasks_PK PRIMARY KEY (id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='etl作业每天表字段主键'
;
create table metadb.sync_columns_info
(
 id                    int  not null AUTO_INCREMENT COMMENT '自增主键'
 ,source_db            varchar(32) COMMENT'来源数据库'
 ,source_table         varchar(50) COMMENT'来源表'
 ,target_db            varchar(32) COMMENT'目标数据库'
 ,target_table         varchar(50) COMMENT'目标表'
 ,column_name          varchar(32) COMMENT'来源字段'
 ,column_type          varchar(32) COMMENT'来源字段类型'
 ,column_comment       varchar(1000) COMMENT'来源字段说明'
 ,column_key           varchar(32) COMMENT'来源字段主键'
 ,is_alter             int COMMENT'是否新增'
 ,status               varchar(32) COMMENT'状态'
,create_time          datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time          datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT sync_tasks_info_tasks_PK PRIMARY KEY (id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='etl同步表字段'
;
create table metadb.sensitive_column
(
id                      int  not null AUTO_INCREMENT COMMENT '自增主键'
,`db`                    varchar(32)  COMMENT'库名'
,`table`                 varchar(50)  COMMENT'表名'
,`column`                varchar(50)  COMMENT'列名'
,sensitive_length        int          COMMENT'加密长度'
,exceed_length           int          COMMENT'预留长度'
,sensitive_type          varchar(50)  COMMENT'加密类型：left、middle_left、middle_right、right、left_word、middle_word、right_word'
,sensitive_value         varchar(50)  COMMENT'加密方法：字符串、MySQL函数，目前仅支持函数只有一个参数，如：md5'
,sensitive_search_word   varchar(50)  COMMENT'按某个字符加密'
,sensitive_value_type    varchar(50)  COMMENT'按某个字符加密类型，左边、右边等'
,status                  int          COMMENT'状态'
,create_time            datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time            datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT sync_tasks_info_tasks_PK PRIMARY KEY (id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='敏感字段配置表'
;

create table metadb.etl_job_dep
(
id                      int  not null AUTO_INCREMENT COMMENT '自增主键'
,task_id                varchar(100)  COMMENT'任务名称'
,dep_task_id            varchar(100)  COMMENT'依赖任务名称'
,status                 int          COMMENT'状态1启用，0停用'
,create_time            datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time            datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT etl_job_dep_PK PRIMARY KEY (id)
,UNIQUE KEY etl_job_dep_unique_ind_task_id (task_id,dep_task_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='作业依赖配置表'
;
insert into metadb.etl_job_dep
(task_id,dep_task_id,status)
select 'dw_test_02','check_dw_test_id_unique',1

-- 创建视图
create view metadb.v_task_info as
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.sync_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  and a.status = 1
  union all
select a.dag_id
       ,a.shell_name as task_id
       ,b.schedule_interval
from metadb.get_day_tc_interface a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.interface_tasks_info_bak a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.interface_account_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1

  union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.etl_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  and a.status = 1
  union all
select a.check_dag_id
       ,a.check_task_id
       ,b.schedule_interval
from metadb.check_table_unique a
inner join metadb.dags_info b
on a.check_dag_id = b.dag_id
where b.status = 1
  and a.check_state = 1
    union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.interface_oe_async_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  and a.status = 1
    union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.interface_sync_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  and a.status = 1
union all
select
	`a`.`dag_id` as `dag_id`,
	`a`.`task_id` as `task_id`,
	`b`.`schedule_interval` as `schedule_interval`
from
	(
		metadb.`interface_tc_async_tasks_info` `a`
		join metadb.`dags_info` `b` on (
			(`a`.`dag_id` = `b`.`dag_id`)
		)
	)
where
	(
		(`b`.`status` = 1)
		and (`a`.`status` = 1)
	)
union all
select
	`a`.`dag_id` as `dag_id`,
	`a`.`task_id` as `task_id`,
	`b`.`schedule_interval` as `schedule_interval`
from
	(
		metadb.`sync_tasks_hive_mysql` `a`
		join metadb.`dags_info` `b` on (
			(`a`.`dag_id` = `b`.`dag_id`)
		)
	)
where
	(
		(`b`.`status` = 1)
		and (`a`.`status` = 1)
	)
 union all
select a.dag_id
       ,a.task_id
       ,b.schedule_interval
from metadb.spider_tasks_info a
inner join metadb.dags_info b
on a.dag_id = b.dag_id
where b.status = 1
  and a.status = 1
;

create table metadb.etl_tasks_info
(
id                    int  not null AUTO_INCREMENT COMMENT '自增主键',
task_id               varchar(100) not null COMMENT 'task唯一标识，格式：【f|d】_【目标表】',
dag_id                varchar(50) not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】_【dag】',
business              varchar(200) not null null COMMENT '所属项目',
dw_level              varchar(16)  not null  COMMENT '任务所属dw层级',
granularity           char(1) not null COMMENT '抽取粒度：F-全量，D-增量',
target_db              varchar(32)  not null COMMENT '目标db',
target_table           varchar(100)  not null COMMENT '目标表',
unique_column         varchar(65)  COMMENT '目标表唯一键',
no_run_time           varchar(30)   COMMENT '错开高峰时间点，格式：0,1,2,3,4,5,6',
life_cycle            bigint  DEFAULT 0 COMMENT '表生命周期，需要保留数据天数，默认为0，则永久保留',
depends_on_past       int(2) default 1 COMMENT '是否依赖上一次周期调度结果，1：是，0：否',
engine_type           varchar(32) null COMMENT '计算引擎：beeline、hive、spark、presto',
yarn_queue            varchar(32) null COMMENT 'task提交到yarn队列',
hive_config_parameter varchar(500) null COMMENT 'hive配置参数',
spark_config_parameter  varchar(500) null COMMENT 'spark配置参数',
spark_num_executors    varchar(32) null COMMENT 'executors个数',
spark_executor_cores   varchar(32) null COMMENT 'executors核数',
spark_executor_memory  varchar(32) null COMMENT 'executors内存',
spark_driver_memory    varchar(32) null COMMENT 'driver内存',
spark_sql_shuffle_partitions varchar(32) null COMMENT 'shuffle partition个数',
execution_timeout     int(8) null COMMENT 'task运行超时时长（分钟）',
petitioner             varchar(32)  COMMENT '需求提出方',
operator               varchar(32)  COMMENT '任务负责人',
status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
comments              varchar(512)null COMMENT '备注',
create_user           varchar(32)   COMMENT '创建者',
update_user           varchar(32)   COMMENT '最后更新者',
create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
CONSTRAINT etl_tasks_info_tasks_PK PRIMARY KEY (id),
UNIQUE KEY etl_tasks_info_unique_ind_task_id (task_id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='etl作业信息表'
;

insert into metadb.etl_tasks_info
(
task_id
,dag_id
,granularity
,target_db
,target_table
,unique_column
,no_run_time
,depends_on_past
,engine_type
,status
,dw_level
,business
)
select 'dw_test_02','day_metadb_etl_scripts_01','D','dw','test','id','0',1,'hive',1,'dw','yk_bigdata_etl_sql'

insert into metadb.sync_tasks_model
(
 task_id
 ,dag_id
 ,business
 ,source_platform
 ,source_handler
 ,source_db
 ,source_table
 ,granularity
 ,inc_column
 ,inc_date_type
 ,inc_date_format
 ,unique_column
 ,no_run_time
 ,life_cycle
 ,depends_on_past
 ,yarn_queue
 ,execution_timeout
 ,description
 ,status
 ,`operator`
 ,comments
 ,create_user
 ,update_user
)
select 'etl_metadb_conn_db_info' as task_id
,'day_sync_etl_metadb_auto'  as  dag_id
,'etl' as business
,'mysql'  as source_platform
,'etl_metadb' as source_handler
,'metadb' as source_db
,'conn_db_info' as source_table
,'F' as granularity
 ,null as inc_column
 ,null as inc_date_type
 ,null as inc_date_format
,'id' as unique_column
,'0' as no_run_time
,null as life_cycle
,1 as depends_on_past
,null as yarn_queue
,null as execution_timeout
,null as description
,1 as status
 ,'' as operator
 ,'' as comments
 ,'' as create_user
 ,'' as update_user


create table metadb.check_table_unique
(
  id                            int  not null AUTO_INCREMENT COMMENT '自增主键'
  ,check_dag_id                 varchar(32) not null COMMENT '数据质量task所属dag'
  ,check_task_id                varchar(50) not null COMMENT '数据质量task所属dag'
  ,check_platform_type          varchar(30) not null COMMENT '平台类型：mysql、hive'
  ,check_handler                varchar(30) not null COMMENT 'handler连接池'
  ,check_schema                 varchar(32) not null COMMENT '数据库schema名称'
  ,check_db                     varchar(35) not null COMMENT '数据库db名称'
  ,check_table                  varchar(50) not null COMMENT '数据库表名'
  ,check_column                 varchar(50) not null COMMENT '字段名称'
  ,check_filter                 varchar(500) not null COMMENT '常量筛选条件，规则：字段名 = ##固定值##，如：name = ##jack##'
  ,check_filter_date            varchar(500) not null COMMENT '变量筛选日期条件，规则：&&日期字段名=日期类型(TimeStamp\DateTime\Date)=日期格式(0：不带杠，1：横杠，2：斜杠)&&，如：&&create_time=TimeStamp=0&&'
  ,check_custom_sql             varchar(1000) not null COMMENT '自定义sql'
  ,status_level                 varchar(30) not null COMMENT '异常状态级别：red任务中断并发预警，yellow任务成功并发预警，green任务成功不发预警'
  ,spark_config_parameter       varchar(500) null COMMENT 'spark配置参数'
  ,spark_num_executors          varchar(32) null COMMENT 'executors个数'
  ,spark_executor_cores         varchar(32) null COMMENT 'executors核数'
  ,spark_executor_memory        varchar(32) null COMMENT 'executors内存'
  ,spark_driver_memory          varchar(32) null COMMENT 'driver内存'
  ,spark_sql_shuffle_partitions varchar(32) null COMMENT 'shuffle partition个数'
  ,check_state                  int not null COMMENT '状态：1启用，0停用'
  ,pusher                       varchar(100) not null COMMENT '企业微信推送人'
  ,comments              varchar(512)null COMMENT '备注'
  ,create_user           varchar(32) not null COMMENT '创建者'
  ,update_user           varchar(32) not null COMMENT '最后更新者'
  ,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
  ,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
  ,CONSTRAINT check_table_unique_PK PRIMARY KEY (id)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='表字段唯一性检测配置表'
;

create table metadb.interface_tasks_model(
task_id               varchar(100)  not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写'
,dag_id                varchar(50)   not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】'
,interface_acount_type int COMMENT '101腾讯、102微信、2巨量'
,interface_url         varchar(200)  not null comment '接口url'
,interface_level       varchar(200)  comment '接口level'
,interface_time_line   varchar(200)  comment '接口time_line'
,group_by              varchar(200)  comment '接口指定聚合字段'
,is_run_date           int DEFAULT 1 not null comment '是否需要指定日期过滤，1是，0否'
,is_delete             int DEFAULT 0 not null comment '0：接口不需传参，1：true，2：false'
,target_handle         varchar(200) not null comment'连接目标平台handle'
,target_db             varchar(200) not null comment'目标库'
,target_table          varchar(200) not null comment'目标表'
,status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效'
,create_user           varchar(32)   COMMENT '创建者，邮箱@前缀'
,update_user           varchar(32)   COMMENT '最后更新者，邮箱@前缀'
,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT interface_tasks_model_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接口作业模板表'
;

create table metadb.interface_tasks_info(
task_id               varchar(100)  not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写'
,tasks_model_id       varchar(100)  not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写'
,dag_id                varchar(50)   not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】'
,interface_acount_type int COMMENT '101腾讯、102微信、2巨量'
,interface_url         varchar(200)  not null comment '接口url'
,interface_level       varchar(200)  comment '接口level'
,interface_time_line   varchar(200)  comment '接口time_line'
,group_by              varchar(200)  comment '接口指定聚合字段'
,is_run_date           int DEFAULT 1 not null comment '是否需要指定日期过滤，1是，0否'
,is_delete             int DEFAULT 0 not null comment '0：接口不需传参，1：true，2：false'
,sync_level            varchar(20) not null comment'同步层级：file：文件落地至hive，ods：落地至ods库，snap：落地至snap库'
,source_handle         varchar(200) not null comment'连接来源平台handle'
,source_db             varchar(200) not null comment'来源库'
,source_table          varchar(200) not null comment'来源表'
,target_handle         varchar(200) not null comment'连接目标平台handle'
,target_db             varchar(200) not null comment'目标库'
,target_table          varchar(200) not null comment'目标表'
,status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效'
,create_user           varchar(32)   COMMENT '创建者，邮箱@前缀'
,update_user           varchar(32)   COMMENT '最后更新者，邮箱@前缀'
,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT interface_tasks_model_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接口作业配置表'
;

create table metadb.interface_tasks_info_bak(
task_id                varchar(100)  not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写'
,tasks_model_id        varchar(100)  not null COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写'
,dag_id                varchar(50)   not null COMMENT 'dag唯一标识，格式：【hour|day】_【业务名】_【auto】'
,interface_module      varchar(200)  not null comment '接口模块'
,interface_url         varchar(200)  not null comment '接口url'
,data_json             varchar(2000) comment'接口访问参数'
,file_dir_name         varchar(50) comment'文件目录参数名称'
,start_date_name       varchar(50) comment'开始日期字段名称'
,end_date_name         varchar(50) comment'开始日期字段名称'
,init_start_date       varchar(50) comment'初始开始日期'
,init_end_date         varchar(50) comment'初始结束日期'
,filter_modify_time_name varchar(50) comment'过滤更新日期字段名称'
,is_init_data          int comment'是否初始数据，1是，0否'
,sync_level            varchar(20) not null comment'同步层级：file：文件落地至hive，ods：落地至ods库，snap：落地至snap库'
,source_handle         varchar(200)   comment'连接来源平台handle'
,source_db             varchar(200)  comment'来源库'
,source_table          varchar(200)  comment'来源表'
,target_handle         varchar(200) comment'连接目标平台handle'
,target_db             varchar(200)   comment'目标库'
,target_table          varchar(200)   comment'目标表'
,status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效'
,create_user           varchar(32)   COMMENT '创建者，邮箱@前缀'
,update_user           varchar(32)   COMMENT '最后更新者，邮箱@前缀'
,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT interface_tasks_model_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接口作业配置表'
;

insert into metadb.interface_tasks_info_bak
(
task_id
,tasks_model_id
,dag_id
,interface_module
,interface_url
,data_json
,file_dir_name
,start_date_name
,end_date_name
,init_start_date
,init_end_date
,is_init_data
,sync_level
,status
,target_handle
,target_db
,target_table
)
select 'test','test','day_tc_interface_auto_test','oceanengine','http://bd.ec.net/internal/oe/getCampaign',
'{"mt":2,"ec_fn":"/home/server/cloud/dataserver/logs/abc111.log"}','ec_fn','','','',''
,0,'file',1,'hive','etl_mid','oe_getcampaign'
;

 insert into metadb.interface_tasks_info_bak
(
task_id
,tasks_model_id
,dag_id
,is_init_data
,sync_level
,status
,source_handle
,source_db
,source_table
,target_handle
,target_db
,target_table
,interface_module
,interface_url
)
select 'ods_test','test','day_tc_interface_auto_test',0,'ods',1,'beeline','etl_mid','oe_getcampaign','hive','etl_mid','oe_getcampaign','oceanengine'
,''
;


 insert into metadb.dags_info
(dag_id,exec_type,owner,batch_type,retries,schedule_interval,priority_weight,status
)
select 'day_tc_interface_auto_test','interface','etl','day',3,'30 16 * * *',1,1
;

insert into metadb.interface_tasks_model
(task_id,dag_id,interface_acount_type,interface_url,interface_level,interface_time_line,group_by,is_run_date,status
,target_handle,
target_db,target_table,is_delete
)
select 'tc_interface_adcreatives'
       ,'day_tc_interface_auto_test',101
       ,'http://dtapi.ecsage.net/internal/gdt/getAdcreatives',
'REPORT_LEVEL_MATERIAL_IMAGE','REQUEST_TIME','date,ad_id',1,1,'beeline','etl_mid','tc_getadcreatives_adcreatives',0

CREATE TABLE `get_day_tc_interface` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `dag_id` varchar(100) DEFAULT NULL,
  `shell_name` varchar(1000) DEFAULT NULL,
  `shell_path` varchar(500) DEFAULT NULL,
  `params` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='接口任务配置'

CREATE TABLE metadb.oe_async_task_interface (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `dag_id` varchar(1000) DEFAULT NULL COMMENT '执行接口所属dag',
  `dag_task_id` varchar(1000) DEFAULT NULL COMMENT '执行接口所属dag的taskId',
  `media_type` varchar(100) DEFAULT NULL COMMENT '媒体子账户类型',
  `token_data` varchar(1000) DEFAULT NULL COMMENT 'token',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `account_id`   varchar(1000) DEFAULT NULL COMMENT '子账户id',
  `task_id`   varchar(1000) DEFAULT NULL COMMENT '异步任务id',
  `task_name`   varchar(1000) DEFAULT NULL COMMENT '异步任务名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='巨量异步任务'


CREATE TABLE metadb.`oe_async_task_interface` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   media_type int(11) comment'',
  `token_data` varchar(1000) DEFAULT NULL COMMENT 'token',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `account_id` varchar(1000) DEFAULT NULL COMMENT '子账户id',
  `task_id` varchar(1000) DEFAULT NULL COMMENT '异步任务id',
  `task_name` varchar(1000) DEFAULT NULL COMMENT '异步任务名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=81922 DEFAULT CHARSET=utf8 COMMENT='巨量异步任务'


AccountId, MediaType,ServiceCode, Token

CREATE TABLE metadb.oe_valid_account_interface (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   media_type int(11) comment'',
  `token_data` varchar(1000) DEFAULT NULL COMMENT 'token',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `account_id` varchar(1000) DEFAULT NULL COMMENT '子账户id',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='巨量有效子账户'


CREATE TABLE metadb.request_hostname_interface (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   hostname varchar(1000) not null comment'请求域名',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='请求域名表'

AccountTokenFile

CREATE TABLE metadb.request_account_token_interface (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   media_type int(11) not null comment'',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `token_data` varchar(1000) DEFAULT NULL COMMENT 'token',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='请求子账户token表'

account_id, media, service_code

CREATE TABLE metadb.request_account_interface (
  `id` int(11)  COMMENT '主键',
   media_type int(11) not null comment'',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `account_id` varchar(1000) DEFAULT NULL COMMENT '子账户'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='请求子账户表'


CREATE TABLE metadb.request_account_host (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   ip varchar(200) not null comment'',
  `user_name` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `passwd` varchar(1000) DEFAULT NULL COMMENT '子账户',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='子账户服务'

CREATE TABLE metadb.interface_account_tasks_info (
  `id`                  int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  media_id              int(10) not null comment'所属媒体id，1：巨量',
  media_type            int(10) not null comment'所属媒体平台类型：2、201、203',
  task_id               varchar(200) not null comment'任务名称',
  dag_id                varchar(200) not null comment'dag名称',
  is_create_task        int(10) not null comment'是否是创建任务，1：是，0：否',
  status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效',
  comments              varchar(512)null COMMENT '备注',
  create_user           varchar(32)   COMMENT '创建者',
  update_user           varchar(32)   COMMENT '最后更新者',
  create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
  update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='子账户任务配置表'

select media_id
       ,media_type
       ,task_id
       ,dag_id
       ,is_create_task
       ,status
       ,comments
       ,create_user
       ,update_user
       ,create_time
       ,update_time
from metadb.interface_account_tasks_info
where status = 1

CREATE TABLE `interface_oe_async_tasks_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `media_type` int(10) NOT NULL COMMENT '所属媒体平台类型：2、201、203，只有是采集接口时必须填',
  `task_id` varchar(200) NOT NULL COMMENT '任务名称',
  `dag_id` varchar(200) NOT NULL COMMENT 'dag名称',
  `task_type` int(10) NOT NULL COMMENT '任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步',
   source_handle  varchar(200) comment'来源端handle',
   source_db      varchar(200) comment'来源端db',
   source_table   varchar(200) comment'来源端表',
   target_handle  varchar(200) comment'目标端handle',
   target_db      varchar(200) comment'目标端db',
   target_table   varchar(200) comment'目标端表',
   interface_group_by varchar(1000) comment'接口groupby字段，多个字段以英文逗号分隔',
   key_columns    varchar(200) comment'业务主键字段，多个字段以英文逗号分隔',
   select_exclude_columns varchar(200) comment'剔除字段，多个字段以英文逗号分隔',
  `status` int(2) NOT NULL DEFAULT '0' COMMENT '是否有效，1：有效，0：无效',
  `comments` varchar(512) DEFAULT NULL COMMENT '备注',
  `create_user` varchar(32) DEFAULT NULL COMMENT '创建者',
  `update_user` varchar(32) DEFAULT NULL COMMENT '最后更新者',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='头条异步任务配置表'

insert into metadb.interface_oe_async_tasks_info (
  `media_type` -- int(10) NOT NULL COMMENT '所属媒体平台类型：2、201、203，只有是采集接口时必须填',
  ,`task_id`  -- varchar(200) NOT NULL COMMENT '任务名称',
  ,`dag_id` -- varchar(200) NOT NULL COMMENT 'dag名称',
  ,`task_type` -- int(10) NOT NULL COMMENT '任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步',
  , target_handle -- varchar(200) comment'目标端handle',
  , target_db    --  varchar(200) comment'目标端db',
  , target_table --  varchar(200) comment'目标端表',
  , key_columns   -- varchar(200) comment'业务主键字段，多个字段以英文逗号分隔',
  ,`status`  -- int(2) NOT NULL DEFAULT '0' COMMENT '是否有效，1：有效，0：无效',
)
select '2','etl_mid_oe_getcreativereport_creativereport','day_oe_async_creativereport_interface_auto',2,'hive','etl_mid','oe_getcreativereport_creativereport'
,'creative_id',1

create table metadb.oe_service_token_interface(
service_code  varchar(100)  comment'服务商'
,token_code   varchar(500)  comment'token'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='头条代理商token'


create table metadb.media_advertiser(
media_type    int   comment'服务商类型'
,account_id     varchar(500)  comment'子账户'
,service_code  varchar(100)  comment'服务商'
,token_code   varchar(500)  comment'token'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='头条子账户表'


CREATE TABLE metadb.`interface_oe_async_tasks_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `media_type` varchar(100) NOT NULL COMMENT '所属媒体平台类型：2、201、203，只有是采集接口时必须填',
  `task_id` varchar(200) NOT NULL COMMENT '任务名称',
  `dag_id` varchar(200) NOT NULL COMMENT 'dag名称',
  `task_type` int(10) NOT NULL COMMENT '任务类型，1：创建异步任务，0：获取异步任务状态，2：获取异步任务数据，3：ods同步，4：snap同步',
  `source_handle` varchar(200) DEFAULT NULL COMMENT '来源端handle',
  `source_db` varchar(200) DEFAULT NULL COMMENT '来源端db',
  `source_table` varchar(200) DEFAULT NULL COMMENT '来源端表',
  `target_handle` varchar(200) DEFAULT NULL COMMENT '目标端handle',
  `target_db` varchar(200) DEFAULT NULL COMMENT '目标端db',
  `target_table` varchar(200) DEFAULT NULL COMMENT '目标端表',
  `interface_group_by` varchar(1000) DEFAULT NULL COMMENT '接口groupby字段，多个字段以英文逗号分隔',
  `key_columns` varchar(200) DEFAULT NULL COMMENT '业务主键字段，多个字段以英文逗号分隔',
  `select_exclude_columns` varchar(200) DEFAULT NULL COMMENT '剔除字段，多个字段以英文逗号分隔',
  `status` int(2) NOT NULL DEFAULT '0' COMMENT '是否有效，1：有效，0：无效',
  `comments` varchar(512) DEFAULT NULL COMMENT '备注',
  `create_user` varchar(32) DEFAULT NULL COMMENT '创建者',
  `update_user` varchar(32) DEFAULT NULL COMMENT '最后更新者',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8 COMMENT='头条异步任务配置表'

CREATE TABLE metadb.oe_account_interface (
  `account_id` varchar(80) DEFAULT NULL COMMENT '子账户id',
  `media_type` int(11) DEFAULT NULL,
  `service_code` varchar(80) DEFAULT NULL COMMENT '代理商凭证',
  `token_data` varchar(100) DEFAULT NULL COMMENT 'token',
  `exec_date` varchar(40) DEFAULT NULL COMMENT '筛选日期',
  KEY `index_exec_date_account_id` (`exec_date`,`account_id`),
  KEY `index_exec_date` (`exec_date`),
  KEY `index_exec_date_media_type` (`exec_date`,`media_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `oe_async_create_task_interface` (
  `media_type` int(11) DEFAULT NULL,
  `token_data` varchar(1000) DEFAULT NULL COMMENT 'token',
  `service_code` varchar(500) DEFAULT NULL COMMENT '代理商凭证',
  `account_id` varchar(1000) DEFAULT NULL COMMENT '子账户id',
  `task_id` varchar(1000) DEFAULT NULL COMMENT '异步任务id',
  `task_name` varchar(1000) DEFAULT NULL COMMENT '异步任务名称',
  `interface_flag` varchar(500) DEFAULT NULL COMMENT '异步任务接口标识',
   group_by  varchar(1000) comment'接口groupby',
   fields   longtext comment'接口指定指标字段'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='巨量异步创建任务'






CREATE TABLE `interface_sync_tasks_info` (
  `task_id` varchar(100) NOT NULL COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写',
  `tasks_model_id` varchar(100) NOT NULL COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写',
  `dag_id` varchar(500) DEFAULT NULL,
  `interface_module` varchar(200) NOT NULL COMMENT '接口模块',
  `interface_url` varchar(200) NOT NULL COMMENT '接口url',
  `data_json` varchar(2000) DEFAULT NULL COMMENT '接口访问参数',
  `start_date_name` varchar(50) DEFAULT NULL COMMENT '开始日期字段名称',
  `end_date_name` varchar(50) DEFAULT NULL COMMENT '结束日期字段名称',
  `filter_modify_time_name` varchar(50) DEFAULT NULL COMMENT '过滤更新日期字段名称',
  `sync_level` varchar(20) NOT NULL COMMENT '同步层级：file：文件落地至hive，ods：落地至ods库，snap：落地至snap库',
  `source_handle` varchar(200) DEFAULT NULL COMMENT '连接来源平台handle',
  `source_db` varchar(200) DEFAULT NULL COMMENT '来源库',
  `source_table` varchar(200) DEFAULT NULL COMMENT '来源表',
  `target_handle` varchar(200) DEFAULT NULL COMMENT '连接目标平台handle',
  `target_db` varchar(200) DEFAULT NULL COMMENT '目标库',
  `target_table` varchar(200) DEFAULT NULL COMMENT '目标表',
  `status` int(2) NOT NULL DEFAULT '0' COMMENT '是否有效，1：有效，0：无效',
  `select_exclude_columns` varchar(2000) DEFAULT NULL COMMENT '查询排除字段',
  `is_report` int(11) DEFAULT NULL COMMENT '是否报表，1:是，0:否',
  `key_columns` varchar(2000) DEFAULT NULL COMMENT '目标表主键，多个字段以英文逗号分隔。当is_report字段为1时，此字段必须填',
  `exclude_account_id` varchar(500) DEFAULT NULL COMMENT '过滤掉指定子账户',
  filter_db_name   varchar(200) DEFAULT NULL COMMENT '过滤指定库',
  filter_table_name   varchar(200) DEFAULT NULL COMMENT '过滤指定表',
  filter_column_name   varchar(200) DEFAULT NULL COMMENT '过滤指定字段',
  `create_user` varchar(32) DEFAULT NULL COMMENT '创建者，邮箱@前缀',
  `update_user` varchar(32) DEFAULT NULL COMMENT '最后更新者，邮箱@前缀',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='同步接口作业配置表';

CREATE TABLE `sync_tasks_hive_mysql` (
  `task_id` varchar(100) NOT NULL COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写',
  `tasks_model_id` varchar(100) NOT NULL COMMENT 'task唯一标识，格式：【业务名】_【源库名】_【源表名】,字母则为小写',
  `dag_id` varchar(500) DEFAULT NULL,
  `interface_module` varchar(200) NOT NULL COMMENT '接口模块',
  `source_handle` varchar(200) DEFAULT NULL COMMENT '连接来源平台handle',
  `source_db` varchar(20) DEFAULT NULL COMMENT '来源库',
  `source_table` varchar(200) DEFAULT NULL COMMENT '来源表',
  `target_handle` varchar(200) DEFAULT NULL COMMENT '连接目标平台handle',
  `target_db` varchar(20) DEFAULT NULL COMMENT '目标库',
  `sync_level` varchar(20) NOT NULL COMMENT '同步层级',
  `target_table` varchar(200) DEFAULT NULL COMMENT '目标表',
  `export_mode` varchar(20) DEFAULT NULL COMMENT '导出方式(增量1全量0)',
  `increment_mode` varchar(20) DEFAULT NULL COMMENT '增量方式(年0月1日2)',
  `increment_columns` varchar(20) DEFAULT NULL COMMENT '增量字段',
  `filter_condition` varchar(200) DEFAULT NULL COMMENT '过滤条件',
  `column_identical` int(2) DEFAULT NULL COMMENT '是否按照mysql 表结构导出(mysql表结构和hive表结构一样),1：一致,0:不一致',
  `export_columns` varchar(2000) DEFAULT NULL COMMENT '如不一致,导出字段',
  `status` int(2) NOT NULL DEFAULT '0' COMMENT '是否有效，1：有效，0：无效',
  `key_columns` varchar(2000) DEFAULT NULL COMMENT '目标表主键，多个字段以英文逗号分隔',
  `create_user` varchar(32) DEFAULT NULL COMMENT '创建者，邮箱@前缀',
  `update_user` varchar(32) DEFAULT NULL COMMENT '最后更新者，邮箱@前缀',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hive同步mysql作业配置表';

create  table spider_tasks_info(
task_id                varchar(200) not null  comment'爬虫任务id'
,dag_id                varchar(200) not null  comment'爬虫任务dag id'
,spider_id             varchar(100) not null  comment'scrapy爬虫名'
,platform_id           varchar(200) not null  comment'爬虫数据所属平台id'
,platform_name         varchar(500) not null  comment'爬虫数据所属平台名称'
,module_id             varchar(200) not null  comment'爬虫数据所属平台模块id'
,module_name           varchar(500) not null  comment'爬虫数据所属平台模块名称'
,url                   varchar(200) not null  comment'爬虫数据所属平台url地址'
,data_level            varchar(20)  not null comment'同步层级：file：文件落地至hive，ods：落地至ods库，snap：落地至snap库'
,source_handle         varchar(200)   comment'连接来源平台handle'
,source_db             varchar(200)  comment'来源库'
,source_table          varchar(200)  comment'来源表'
,target_handle         varchar(200) comment'连接目标平台handle'
,target_db             varchar(200)   comment'目标库'
,target_table          varchar(200)   comment'目标表'
,key_columns           varchar(200)   comment'主键字段，多个字段以英文逗号分隔'
,is_report             varchar(10)    comment'是否报表'
,status                int(2)  DEFAULT 0 not null COMMENT '是否有效，1：有效，0：无效'
,create_user           varchar(32)   COMMENT '创建者，邮箱@前缀'
,update_user           varchar(32)   COMMENT '最后更新者，邮箱@前缀'
,create_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳'
,update_time           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间戳'
,CONSTRAINT spider_tasks_info_tasks_PK PRIMARY KEY (task_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='爬虫作业配置表'
;


