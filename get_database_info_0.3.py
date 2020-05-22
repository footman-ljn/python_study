import argparse
import pymysql
import cx_Oracle
import pymssql
import psycopg2
import jinja2
from decimal import Decimal

def check_param(in_plist):
    ck_rnum = 0
    return ck_rnum

def check_action(in_alist):
    ck_rnum = 0
    return ck_rnum

def action_sql_for_oracle(in_action):
    if in_action == 'ver_info':
        sql = 'select * from v$version'
    elif in_action == 'dbsp_info':
        sql = ''
    elif in_action == 'tbsp_info':
        sql = ''
    return sql

def action_sql_for_mysql(in_action):
    if in_action == 'ver_info':
        sql = 'select version()'
    elif in_action == 'dbsp_info':
        sql = '''SELECT 
table_schema,
ROUND(SUM(data_length / 1024 / 1024), 2) AS data_length_MB,
ROUND(SUM(index_length / 1024 / 1024), 2) AS index_length_MB,
ROUND(SUM((data_length + index_length) / 1024 / 1024), 2) AS total_length_MB
FROM information_schema.tables
GROUP BY table_schema
ORDER BY data_length_MB DESC , index_length_MB DESC'''
    elif in_action == 'tbsp_info':
        sql = '''SELECT t.table_name 表,t.table_schema 库,t.engine 引擎,t.table_length_B 表空间,
t.table_length_B/t1.all_length_B 表空间占比,t.data_length_B 数据空间,
t.index_length_B 索引空间,t.table_rows 行数,t.avg_row_length_B 平均行长KB
FROM (SELECT table_name,table_schema,ENGINE,table_rows,
data_length +  index_length AS table_length_B,
data_length AS data_length_B,index_length AS index_length_B,
AVG_ROW_LENGTH AS avg_row_length_B
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql' , 'performance_schema', 'information_schema', 'sys')) t
join (select sum((data_length + index_length)) as all_length_B from information_schema.tables) t1'''
    return sql

def action_sql_for_mssql(in_action):
    if in_action == 'ver_info':
        sql = 'select @@version'
    elif in_action == 'dbsp_info':
        sql = ''
    elif in_action == 'tbsp_info':
        sql = ''
    return sql

def action_sql_for_pg(in_action):
    if in_action == 'ver_info':
        sql = 'show server_version'
    elif in_action == 'dbsp_info':
        sql = ''
    elif in_action == 'tbsp_info':
        sql = ''
    return sql

def conn_database(in_plist,in_alist):
    v_dbengine, v_dip, v_duser, v_dpass, v_dport, v_ddb, v_dsname = in_plist
    v_dport = int(v_dport)
    v_dbengine = v_dbengine.lower()
    tmp_res = []
    if v_dbengine == 'oracle':
        d_linkstr = v_dip + ':' + str(v_dport) + '/' + v_dsname
        conn = cx_Oracle.connect(v_duser, v_dpass, d_linkstr)
        for v_action in in_alist:
            sql = action_sql_for_oracle(v_action)
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            tmp_res.append(data)
    elif v_dbengine == 'mysql':
        conn =pymysql.connect(host=v_dip, user=v_duser, passwd=v_dpass, port=v_dport, db=v_ddb, charset='utf8')
        for v_action in in_alist:
            sql = action_sql_for_mysql(v_action)
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            tmp_res.append(data)
    elif v_dbengine == 'mssql':
        conn = pymssql.connect(host=v_dip, user=v_duser, password=v_dpass, database=v_ddb, port=v_dport)
        for v_action in in_alist:
            sql = action_sql_for_mssql(v_action)
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            tmp_res.append(data)
    elif v_dbengine == 'pgsql':
        conn = psycopg2.connect(host=v_dip, user=v_duser, password=v_dpass, database=v_ddb, port=v_dport)
        for v_action in in_alist:
            sql = action_sql_for_pg(v_action)
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            tmp_res.append(data)
    return tmp_res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''数据库连接工具
    欢迎使用！数据库连接工具（伊斯）
支持的数据库类型：Oracle、MySQL、MSSQL、PostgreSQL
版本号：V 0.3 Bate
更新日期：2020-04-16
使用样例：
python get_database_info_0.2.py 
--Engine mysql 
--Host 172.10.1.5 
--User test 
--Password test 
--Port 3306 
--Database testdb''', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--Engine", help='''数据库类型 必要参数
只能选择其中一种参数：
Oracle     => oracle
MySQL      => mysql
SQLServer  => mssql
PostgreSQL => pgsql''')
    parser.add_argument("--Host", help="指定目标数据库服务器IP地址 必要参数")
    parser.add_argument("--User", help="指定连接数据库的用户名 必要参数")
    parser.add_argument("--Password", help="指定用户名对应的密码 必要参数")
    parser.add_argument("--Port", help="指定目标数据库服务器端口 必要参数")
    parser.add_argument("--Database", help="连接的库名（该参数不适用于Oracle数据库）")
    parser.add_argument("--Service_name", help="指定连接数据库的服务名（仅限Oracle数据库）")
    parser.add_argument("--Action", help='''选择执行动作，可多选，用逗号','分隔
查询版本信息      => ver_info
查询库空间信息    => dbsp_info
查询表空间信息    => tbsp_info''')
    parser.add_argument("--OutFile", help="输出的文件名")

args = parser.parse_args()
# 入参整合
l_param = []
l_param.append(args.Engine)
l_param.append(args.Host)
l_param.append(args.User)
l_param.append(args.Password)
l_param.append(args.Port)
l_param.append(args.Database)
l_param.append(args.Service_name)
l_action = args.Action.split(',')
o_file = args.OutFile

#print(l_param,l_action,sep='\n')
all_data = conn_database(l_param,l_action)
#print(all_data)
l_version = ()
l_dbsp = ()
l_tbsp = ()
len_version = 0
len_dbsp = 0
len_tbsp = 0

for i in l_action:
    if i == 'ver_info':
        l_version = all_data[l_action.index(i)]
        len_version = 1
    elif i == 'dbsp_info':
        l_dbsp = all_data[l_action.index(i)]
        len_dbsp = 1
    elif i == 'tbsp_info':
        l_tbsp = all_data[l_action.index(i)]
        len_tbsp = 1

#print(len_version,len_dbsp,len_tbsp,l_version,l_dbsp,l_tbsp,sep='\n')

loader = jinja2.FileSystemLoader(searchpath='.\\')
env = jinja2.Environment(loader = loader)
template = env.get_template("mode.html",'utf-8')
html = template.render(len_version=len_version,
len_dbsp=len_dbsp,
len_tbsp=len_tbsp,
l_version=l_version,
l_dbsp=l_dbsp,
l_tbsp=l_tbsp)

with open(o_file,'w') as fp:
    fp.write(html)
#print(f2)

