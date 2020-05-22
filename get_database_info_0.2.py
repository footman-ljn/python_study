# -*- coding: utf-8 -*-
import argparse
import pymysql
import cx_Oracle
import pymssql
import psycopg2


def conn_mysql():
    '''
    用于连接 MySQL 数据库
    获取 MySQL 数据库的版本信息
    '''
    d_ip = '10.200.6.53'
    d_user = 'root'
    d_pass = 'root'
    d_port = 3306
    d_db = 'test01'
    conn = pymysql.connect(host=d_ip,
                           user=d_user,
                           passwd=d_pass,
                           port=d_port,
                           db=d_db,
                           charset='utf8'
                           )
    cur = conn.cursor()
    sql = "select version()"
    cur.execute(sql)
    data = cur.fetchall()
    v_info = ''
    for i in data:
        v_info = v_info + i[0] + '\n'
    cur.close()
    conn.close()
    return v_info


def conn_oracle():
    '''
    用于连接 Oracle 数据库
    并获取 Oracle 版本号
    '''
    d_ip = '10.200.6.53'
    d_user = 'captureuser'
    d_pass = 'captureuser'
    d_port = 1522
    d_sname = 'testdb.zhuyun'
    d_linkstr = d_ip + ':' + str(d_port) + '/' + d_sname
    # print(d_linkstr)
    conn = cx_Oracle.connect(d_user, d_pass, d_linkstr)
    cur = conn.cursor()
    sql = 'select * from v$version'
    cur.execute(sql)
    data = cur.fetchall()
    v_info = ''
    for i in data:
        v_info = v_info + i[0] + '\n'
    cur.close()
    conn.close()
    return v_info


def conn_mssql():
    d_ip = '10.200.6.55'
    d_user = 'sa'
    d_pass = 'Zyadmin@123'
    d_db = 'testdb'
    d_port = 1433
    conn = pymssql.connect(host=d_ip, user=d_user, password=d_pass, database=d_db, port=d_port)
    cur = conn.cursor()
    sql = 'select @@version'
    cur.execute(sql)
    data = cur.fetchall()
    v_info = ''
    for i in data:
        v_info = v_info + i[0] + '\n'
    cur.close()
    conn.close()
    return v_info


def conn_pgsql():
    d_ip = '172.10.1.5'
    d_user = 'postgres'
    d_pass = 'postgres'
    d_db = 'testdb'
    d_port = 5432
    conn = psycopg2.connect(host=d_ip, user=d_user, password=d_pass, database=d_db, port=d_port)
    cur = conn.cursor()
    sql = 'show server_version'
    cur.execute(sql)
    data = cur.fetchall()
    v_info = ''
    for i in data:
        v_info = v_info + i[0] + '\n'
    cur.close()
    conn.close()
    return v_info


def chk_args(in_args):
    '''
    用于检查各参数的合法性
    return: 0 参数检查通过，参数均合法
    return: >0 参数检查失败，存在不合法参数
    '''
    re_code = 0
    #检查选择的数据库是否在要求范围内
    l_database = ['oracle', 'mysql', 'mssql', 'pgsql']
    if in_args[0].lower() == l_database[0] and (l_inputs[6] != None):
        pass
    elif in_args[0].lower() in l_database[1:] and l_inputs[5] != None:
        pass
    else:
        re_code += 1
    #检查IP是否合规
    v_lip = in_args[1].split('.')
    if len(v_lip) == 4:
        for i in v_lip:
            try:
                lip = int(i)
                if lip >=0 and lip <= 255:
                    pass
                else:
                    re_code += 1
            except Exception as e:
                re_code += 1
    else:
        re_code += 1
    #检查用户名密码参数是否已提供
    for i in in_args[2:4]:
        if i == None:
            re_code += 1
    #检查端口参数是否合规
    try:
        int(in_args[4])
    except Exception as e:
        re_code += 1

    return re_code


def conn_database(v_list):
    v_dbengine, v_dip, v_duser, v_dpass, v_dport, v_ddb, v_dsname = v_list
    v_dport = int(v_dport)
    res_info = ''
    if v_dbengine.lower() == 'oracle':
        d_linkstr =  v_dip + ':' + str(v_dport) + '/' + v_dsname
        conn = cx_Oracle.connect(v_duser, v_dpass, d_linkstr)
        sql = 'select * from v$version'
    elif v_dbengine.lower() == 'mysql':
        conn =pymysql.connect(host=v_dip, user=v_duser, passwd=v_dpass, port=v_dport, db=v_ddb, charset='utf8')
        sql = 'select version()'
    elif v_dbengine.lower() == 'mssql':
        conn = pymssql.connect(host=v_dip, user=v_duser, password=v_dpass, database=v_ddb, port=v_dport)
        sql = 'select @@version'
    elif v_dbengine.lower() == 'pgsql':
        conn = psycopg2.connect(host=v_dip, user=v_duser, password=v_dpass, database=v_ddb, port=v_dport)
        sql = 'show server_version'
    else:
        res_info = '内部错误'

    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    for i in data:
        res_info = res_info + i[0] + '\n'
    cur.close()
    conn.close()

    return res_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''数据库连接工具
    欢迎使用！数据库连接工具（伊斯）
支持的数据库类型：Oracle、MySQL、MSSQL、PostgreSQL
版本号：V 0.2 Bate
更新日期：2020-03-25''', formatter_class=argparse.RawTextHelpFormatter)
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


args = parser.parse_args()
# 入参整合
l_inputs = []
l_inputs.append(args.Engine)
l_inputs.append(args.Host)
l_inputs.append(args.User)
l_inputs.append(args.Password)
l_inputs.append(args.Port)
l_inputs.append(args.Database)
l_inputs.append(args.Service_name)

#检查参数合法性
chk_num = chk_args(l_inputs)
#连接数据库
if chk_num == 0:
    print('%s的数据库版本为%s' % (l_inputs[0], conn_database(l_inputs)))
else:
    print('请检查是否缺失参数或参数不合法')
