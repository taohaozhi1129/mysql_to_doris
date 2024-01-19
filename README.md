# mysql_to_doris
mysql建表语句批量转为doris建表语句

step 1.
生成配置文件并导出
SELECT table_schema,                                     #源库
       table_name,                                       #源表
       'ods_dev'                         AS 'target_db', #目标库
       concat('ods_', table_name, '_df') as 'target_tb', #目标表(命名规则可自定义)
       IF(table_comment = '', table_name, table_comment) #表备注(没有就用源表名)
FROM information_schema.tables
WHERE table_schema = 'test'
  AND table_type = 'BASE TABLE';
![image](https://github.com/taohaozhi1129/mysql_to_doris/assets/57392019/e8ac2681-c01e-4188-bc2d-47677d12f487)

step 2.
run mysql_to_doris.py
