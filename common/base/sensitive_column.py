# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 18:04
# @Author  : wangsong
# @FileName: mysql_2_hive.py
# @Software: PyCharm


from ecsage_bigdata_etl_engineering.common.operator.mysql.conn_mysql_metadb import EtlMetadata

etl_md = EtlMetadata()

def sensitive_column(platform,db, tb, column):
    # 对从ods导入到snap的敏感数据做md5加密
    if db in etl_md.get_sensitive_column():
        if tb in etl_md.get_sensitive_column()[db]:
            if etl_md.get_sensitive_column()[db][tb].count(column.lower()) > 0:
                # 添加位数加密
                ok, column_info = etl_md.execute_sql(sqlName="get_sensitive_sql",Parameter={"db": db, "table": tb, "column": column},IsReturnData="Y")
                sensitive_length = column_info[0][3]
                exceed_length = column_info[0][4]
                sensitive_type = column_info[0][5]  # left、middle_left、middle_right、right、left_word、middle_word、right_word
                sensitive_value = column_info[0][6]  # 字符串、MySQL函数，目前仅支持函数只有一个参数，如：md5
                sensitive_search_word = column_info[0][7]
                sensitive_value_type = column_info[0][8]  # 0：字符串加密，1：MySQL函数加密
                if platform == "hive":
                    columns = """cast(ceil(length(t.%s) / 2) as int)"""%(column)
                else:
                    columns = """ceil(length(t.%s) / 2)""" % (column)
                delite = "@@@@"
                # 左边明文，右边加密
                if sensitive_type == "left":
                    exceed_sensitive_length = sensitive_length + exceed_length
                    if sensitive_length is None or sensitive_length == 0:
                        if sensitive_value_type == "0":
                            sensitive_sql = """repeat('{sensitive_value}',length(t.{column}))""".format(sensitive_value=sensitive_value, column=column)
                        else:
                            sensitive_sql = """{sensitive_value}(t.{column})""".format(sensitive_value=sensitive_value, column=column)
                    else:
                        if sensitive_value_type == "0":
                            sensitive_sql = """ 
                                          case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                                               when  length(t.{column})>={exceed_sensitive_length} then concat(substring(t.{column},1,{sensitive_length}),'{delite}',repeat('{sensitive_value}',length(substring(t.{column},{sensitive_length}+1))))
                                               when length(t.{column})<{exceed_sensitive_length} then concat(substring(t.{column},1,{columns}),'{delite}',repeat('{sensitive_value}',length(substring(t.{column},{columns}+1))))
                                          end
                                          """.format(column=column, exceed_sensitive_length=exceed_sensitive_length,sensitive_length=sensitive_length, sensitive_value=sensitive_value,delite=delite,columns=columns)
                        else:  # md5加密
                            sensitive_sql = """ 
                                            case when t.{column} is null or t.{column} = '' then {sensitive_value}('')
                                                 when  length(t.{column})>={exceed_sensitive_length} then concat(substring(t.{column},1,{sensitive_length}),'{delite}',{sensitive_value}(substring(t.{column},{sensitive_length}+1)))
                                                 when length(t.{column})<{exceed_sensitive_length} then concat(substring(t.{column},1,{columns}),'{delite}',{sensitive_value}(substring(t.{column},{columns}+1)))
                                            end
                            """.format(column=column,exceed_sensitive_length=exceed_sensitive_length,sensitive_length=sensitive_length,sensitive_value=sensitive_value,delite=delite,columns=columns)
                # 按中间左边明文右边加密
                elif sensitive_type == "middle_left":
                    if sensitive_value_type == "0":
                        sensitive_sql = """
                           case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                           else concat(substring(t.{column},1,{columns}),'{delite}',repeat('{sensitive_value}',length(substring(t.{column},{columns}+1))))
                           end
                                        """.format(column=column, sensitive_value=sensitive_value,delite=delite,columns=columns)
                    else:  # md5加密
                        sensitive_sql = """
                        case when t.{column} is null or t.{column} = '' then {sensitive_value}('')
                        else concat(substring(t.{column},1,{columns}),'{delite}',{sensitive_value}(substring(t.{column},{columns}+1)))
                        end
                                        """.format(column=column, sensitive_value=sensitive_value,delite=delite,columns=columns)
                # 按中间左边加密右边明文
                elif sensitive_type == "middle_right":
                    if sensitive_value_type == "0":
                        sensitive_sql = """
                                        case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                                             else concat(repeat('{sensitive_value}',FLOOR(length(t.{column})/2)),'{delite}',substring(t.{column},{columns}))
                                        end
                                       """.format(column=column, sensitive_value=sensitive_value,delite=delite,columns=columns)
                    else:  # md5加密
                        sensitive_sql = """
                                      case when t.{column} is null or t.{column} = '' then {sensitive_value}('')
                                           else concat({sensitive_value}(substring(t.{column},1,FLOOR(length(t.{column})/2))),'{delite}',substring(t.{column},{columns}))
                                      end
                                       """.format(column=column, sensitive_value=sensitive_value,delite=delite,columns=columns)
                # 左边加密右边明文
                elif sensitive_type == "right":
                    exceed_sensitive_length = sensitive_length + exceed_length
                    if sensitive_length is None or sensitive_length == 0:
                        if sensitive_value_type == "0":
                            sensitive_sql = """repeat('{sensitive_value}',length(t.{column}))""".format(sensitive_value=sensitive_value, column=column)
                        else:
                            sensitive_sql = """{sensitive_value}(t.{column})""".format(sensitive_value=sensitive_value, column=column)
                    else:
                        if sensitive_value_type == "0":
                            sensitive_sql = """ 
                                            case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                                                 when  length(t.{column})>={exceed_sensitive_length} then concat(repeat('{sensitive_value}',length(t.{column})-{sensitive_length}),'{delite}',substring(t.{column},-{sensitive_length}))
                                                 when length(t.{column})<{exceed_sensitive_length} then concat(repeat('{sensitive_value}',FLOOR(length(t.{column})/2)),'{delite}',substring(t.{column},-{columns}))
                                            end
                            """.format(column=column,exceed_sensitive_length=exceed_sensitive_length,sensitive_length=sensitive_length,sensitive_value=sensitive_value,delite=delite,columns=columns)
                        else:  # md5加密
                            sensitive_sql = """ 
                                            case when t.{column} is null or t.{column} = '' then {sensitive_value}('') 
                                            when  length(t.{column})>={exceed_sensitive_length} then concat({sensitive_value}(substring(t.{column},1,length(t.{column})-{sensitive_length})),'{delite}',substring(t.{column},-{sensitive_length}))
                                                 when length(t.{column})<{exceed_sensitive_length} then concat({sensitive_value}(substring(t.{column},1,FLOOR(length(t.{column})/2))),'{delite}',substring(t.{column},-{columns}))
                                            end
                            """.format(column=column,exceed_sensitive_length=exceed_sensitive_length,sensitive_length=sensitive_length,sensitive_value=sensitive_value,delite=delite,columns=columns)
                # 按某个字符加密，字符左边明文右边加密
                elif sensitive_type == "left_word":
                    if sensitive_value_type == "0":
                        sensitive_sql = """
                                case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                                when t.{column} like '%{sensitive_search_word}%' then
                                concat(substring(t.{column},1,LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')),'{sensitive_search_word}',repeat('*',length(substring(t.{column},LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')+2))))
                                 else  repeat('{sensitive_value}',length(t.{column}))
                                 end       
                                """.format(column=column, sensitive_value=sensitive_value,sensitive_search_word=sensitive_search_word)
                    else:  # md5加密
                        sensitive_sql = """case when t.{column} is null or t.{column} = '' then {sensitive_value}('')
                         when t.{column} like '%{sensitive_search_word}%' then
                                        concat(substring(t.{column},1,LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')),'{sensitive_search_word}',{sensitive_value}(substring(t.{column},LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')+2)))
                                        else {sensitive_value}(t.{column})
                                        end
                                    """.format(column=column, sensitive_value=sensitive_value,sensitive_search_word=sensitive_search_word)
                # 按某个字符加密，字符左边加密右边明文
                elif sensitive_type == "right_word":
                    if sensitive_value_type == "0":
                        sensitive_sql = """
                                        case when t.{column} is null or t.{column} = '' then repeat('{sensitive_value}',length('1'))
                                        when t.{column} like '%{sensitive_search_word}%' then 
                                        concat(repeat('*',length(substring(t.{column},1,LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')))),'{sensitive_search_word}',substring(t.{column},LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')+2))
                                             else repeat('{sensitive_value}',length(t.{column}))
                                        end
                                        """.format(column=column, sensitive_value=sensitive_value,sensitive_search_word=sensitive_search_word)
                    else:  # md5加密
                        sensitive_sql = """case when t.{column} is null or t.{column} = '' then {sensitive_value}('')
                        when t.{column} like '%{sensitive_search_word}%' then 
                        concat({sensitive_value}(substring(t.{column},1,LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}'))),'{sensitive_search_word}',substring(t.{column},LENGTH(t.{column})-INStr(REVERSE(t.{column}),'{sensitive_search_word}')+2))
                            else {sensitive_value}(t.{column})
                        end
                        """.format(column=column, sensitive_value=sensitive_value,sensitive_search_word=sensitive_search_word)
                else:
                    if sensitive_value_type == "0":
                        sensitive_sql = """repeat('*',length(t.{column}))""".format(sensitive_value=sensitive_value, column=column)
                    else:
                        sensitive_sql = """md5(t.{column})""".format(sensitive_value=sensitive_value,column=column)

                return True, "md5(`" + column + "`) `" + column + "`", sensitive_sql
    return False, "t."+column, "t."+column
