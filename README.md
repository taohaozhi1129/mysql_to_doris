# mysql_to_doris
mysql建表语句批量转为doris建表语句 ETL  

适配doris语法,自动选择key,替换顺序,类型转换,增加字段,自动选择数据模型等
## step 1.  Generate a configuration file and export it

example:  
源库,源表,doris库,doris表,备注  
test,order,ods_dev,ods_order_df,订单表  
test,user,ods_dev,ods_user_df,用户表

![image](https://github.com/taohaozhi1129/mysql_to_doris/assets/57392019/000ec569-86e1-4e43-8bfc-103b1d19319b)

## step 2.  run mysql_to_doris.py
