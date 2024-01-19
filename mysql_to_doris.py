import pymysql


class ColumnEntity:
    def __init__(self, column_name, ordinal_position, data_type, character_maximum_length, column_type, column_key,
                 column_comment):
        self.column_name = column_name
        self.ordinal_position = ordinal_position
        self.data_type = data_type
        self.character_maximum_length = character_maximum_length
        self.column_type = column_type
        self.column_key = column_key
        self.column_comment = column_comment


class TableInfoEntity:
    def __init__(self, mysql_db, mysql_table, doris_db, doris_table, comment):
        self.mysql_db = mysql_db
        self.mysql_table = mysql_table
        self.doris_db = doris_db
        self.doris_table = doris_table
        self.comment = comment


def info_config():
    info_map = {}
    file = open('C:\\test\\test.txt', mode='r', encoding='utf-8')
    for line in file.readlines():
        list = line.strip('\n').split(',')
        mysql_db = list[0]
        mysql_table = list[1]
        doris_db = list[2]
        doris_table = list[3]
        comment = list[4]
        key = mysql_db + '.' + mysql_table
        table_info_entity = TableInfoEntity(mysql_db, mysql_table, doris_db, doris_table, comment)
        info_map[key] = table_info_entity
    # 关闭文件
    file.close()
    return info_map


def table_column_info():
    table_map = {}
    table_schema = "('test')"  # 要查询的库,多个逗号切分
    connection = pymysql.connect(host='localhost', port=3306, user='root',
                                 passwd='123456')
    cursor = connection.cursor()
    sql = ("select table_schema,table_name,column_name,ordinal_position,data_type,character_maximum_length,column_type,"
           "column_key,column_comment from information_schema.columns where table_schema in {}").format(table_schema)
    cursor.execute(sql)
    table_info = cursor.fetchall()

    for tuple in table_info:
        key = tuple[0] + "." + tuple[1]
        column_entity = ColumnEntity(tuple[2], tuple[3], tuple[4], tuple[5], tuple[6], tuple[7], tuple[8])
        if table_map.__contains__(key):
            values = table_map[key]
            values.append(column_entity)
        else:
            list = []
            list.append(column_entity)
            table_map[key] = list

    # 关闭连接
    cursor.close()
    connection.close()
    return table_map


def mysql_type_convert(data_type, character_maximum_length, column_type):
    # 长度小于100 增加6倍，大于100增加3倍
    if data_type.__eq__('char') or data_type.__eq__('varchar'):
        character_maximum_length = character_maximum_length * 6 if character_maximum_length < 100 else character_maximum_length * 3
        if character_maximum_length > 65533: character_maximum_length = 65530
        data_type = ('char({})'.format(character_maximum_length)) if data_type.__eq__('char') else (
            'varchar({})'.format(character_maximum_length))
    # 这两个字段有精度要求
    if data_type.__eq__('datetime') or data_type.__eq__('decimal'): data_type = column_type
    # 特殊类型替换 为了兼容doris
    s = 'string'
    data_type = (data_type.replace('tinytext', s).replace('mediumtext', s).replace('longtext', s)
                 .replace('tinyblob', s).replace('blob', s).replace('mediumblob', s).replace('longblob', s)
                 .replace('tinystring', s).replace('mediumstring', s).replace('longstring', s)
                 .replace('timestamp', 'datetime').replace('enum', s).replace('set', s)
                 .replace('varbinary', s).replace('binary', s).replace('mediumint', 'int')
                 .replace('year', 'varchar(64)').replace('bit', 'char(10)'))
    if data_type.__eq__('time'): data_type = 'varchar(64)'
    if data_type.__eq__('text'): data_type = s
    return data_type


def batch_mysql_to_doris(info_map, table_map):
    out_file = open('C:\\test\\doris_create.sql', mode='a')
    for key, info_entity in info_map.items():
        doris_db = info_entity.doris_db
        doris_table = info_entity.doris_table
        comment = info_entity.comment
        if table_map.__contains__(key):
            column_list = table_map[key]
            head = 'create table if not exists {}.{} ('.format(doris_db, doris_table)
            body = []
            end = []
            pri_list = []
            first_column_name = '`' + column_list[0].column_name + '`'  # 当前表的第一个字段
            for column_entity in column_list:
                column_name = '`' + column_entity.column_name + '`'
                data_type = column_entity.data_type
                character_maximum_length = column_entity.character_maximum_length
                column_type = column_entity.column_type
                column_key = column_entity.column_key
                column_comment = "'" + column_entity.column_comment + "'"
                # 类型转换,兼容doris
                data_type = mysql_type_convert(data_type, character_maximum_length, column_type)
                # 拼接字段
                value = column_name + '  ' + data_type + '  ' + 'comment ' + column_comment + ','
                # 如果当前字段是主键，就调整顺序
                if column_key.__eq__('PRI'):
                    body.insert(0, value)
                    if len(pri_list) > 0:
                        pri_list.insert(0, column_name)
                    else:
                        pri_list.append(column_name)
                else:
                    body.append(value)
            # 增加两个字段
            body.append("data_source  varchar(500) comment '数据来源',")
            body.append("insert_time  datetime comment '数据插入时间'")
            # 如果有主键就使用 unique模型,如果没有主键就使用duplicate模型，默认第一个字段当作key
            # 可自定义添加相关属性
            if len(pri_list) > 0:
                unique_key = ','.join(pri_list)
                end.append("unique key({})".format(unique_key))
                end.append('comment "{}"'.format(comment))
                end.append('distributed by hash({}) buckets 10;'.format(unique_key))
            else:
                end.append("duplicate key({})".format(first_column_name))
                end.append('comment "{}"'.format(comment))
                end.append('distributed by hash({}) buckets 10;'.format(first_column_name))
                # print("当前表无主键,使用duplicate模型,默认第一个字段当作key 库名:{} 表名:{}".format(doris_db, doris_table))
                print("truncate table " + doris_db + "." + doris_table + ";")

            # 拼接整体的建表语句
            create_sql = head + '\n' + '\n'.join(body) + '\n)\n' + '\n'.join(end) + '\n'
            # print("create_ddl:{}".format(create_ddl))
            # 写入文件
            out_file.write(create_sql)
        else:
            print("配置文件有问题,获取不到对应的表 key:{}".format(key))

    # 关闭结果文件
    out_file.close()


if __name__ == '__main__':
    # 读取表信息配置文件
    info_map = info_config()

    # 读取mysql获取表的column
    table_map = table_column_info()

    # 生成doris建表语句
    batch_mysql_to_doris(info_map, table_map)
