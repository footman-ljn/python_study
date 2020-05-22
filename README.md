# python_study

## 0.2版本

基本版

- 可以连接Oracle、MySQL、SQLServer、PG数据库，获得版本信息并打印出来
- 通过参数决定连接哪一种数据库
- 通过参数获取连接信息
- 有简单的参数校验机制

Example

```
get_database_info_0.2.py --Engine mysql --Host 10.12.34.56 --User testuser --Password testpass --Port 3306 --Database test
```

※ 可以通过 --help 获取帮助

## 0.3版本

改进版

- 继承基本版功能
- 添加了新的参数Action
- 由于添加了新参数，源参数校验机制需升级，暂时停用该机制
- 可以通过Action参数指定要执行的动作（版本信息、库空间信息、表空间信息）
- 目前仅实现MySQL数据库的库、表信息获取，其他类型数据库只有版本信息可以获取

Example

```
et_database_info_0.2.py --Engine mysql --Host 10.12.34.56 --User testuser --Password testpass --Port 3306 --Database test --Action ver_info,dbsp_info,tbsp_info
```

※可以通过 --help 获取帮助