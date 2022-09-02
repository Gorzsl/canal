# 基本说明
监听配置sql涉及的表的增删改事件，并对目标表的数据做相应的同步，保证sql运行结果与目标表一致(除数据顺序)

# 配置
## 1.修改启动器配置: application.yml
```yaml
canal.conf:
  mode: tcp #tcp kafka rocketMQ rabbitMQ
  flatMessage: true
  zookeeperHosts:
  syncBatchSize: 1000
  retries: 0
  timeout:
  srcDataSources: # 数据源相关配置
    defaultDS:
      url: jdbc:mysql://localhost:3306/esturay?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong
      username: root
      password: 121212
  canalAdapters:
  - instance: example # canal instance Name or mq topic name
    groups:
    - groupId: g1
      outerAdapters:
      - name: rdb-estuary                               # 指定为rdb-estuary类型同步
        key: mysql1                                     # 指定adapter的唯一key, 与表映射配置中outerAdapterKey对应
        properties:
          jdbc.driverClassName: com.mysql.jdbc.Driver   # 目标库相关配置
          jdbc.url: jdbc:mysql://localhost:3306/esturay?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong
          jdbc.username: root
          jdbc.password: 121212
```
adapter将会自动加载 conf/rdb-estuary 下的所有.yml结尾的配置文件
## 2.适配器表映射文件
修改 conf/rdb-estuary/mytest_user.yml文件:
```yaml
dataSourceKey: defaultDS # 源数据源的key, 对应上面配置的srcDataSources中的值
destination: example     # cannal的instance或者MQ的topic
groupId: g1              # 对应MQ模式下的groupId, 只会同步对应groupId的数据
outerAdapterKey: mysql1  # adapter key, 对应上面配置outAdapters中的key
concurrent: true         # 是否开启并行同步
dbMapping:
  sql: "select t1.id, t1.code, t1.status, t1.remark, t2.prod_code, t2.num, t3.price, t2.num*t3.price amount
        from t_main t1
        left join t_detail t2
        on t1.id = t2.main_id
        left join t_product t3
        on t2.prod_code = t3.prod_code" # sql语句 相关限制参考【语句配置限制】
  targetDb: esturay      # 目标库
  targetTable: t_result  # 目标表
  _id: id                # 主表的主键(或者其他能标识主表唯一行的字段) 对应的目标表中的字段名(带groupBy的情况不需要配置)
  deleteCondition:       # 用于数据同步时目标表删除语句中添加条件(可用于union all视图的同步)
  commitBatch: 3000      # 批量提交的大小
```

# 语句配置限制：
1. 主表不能为子查询语句
2. 只能使用left join即最左表一定要是主表(主要影响主表的删除事件)
3. 关联从表如果是子查询不能有多张表 子查询只能添加where条件 不能改变字段(别名修改/添加字段是可以实现的 但是麻烦)
4. 主sql中不能有where查询条件(从表子查询中可以有where条件但是不推荐, 可能会造成数据同步的不一致, 比如修改了where条件中的字段内容)
5. 关联条件只允许主外键的'='操作不能出现其他常量判断比如: on a.role_id=b.id and b.statues=1，在关联条件上不可做任何计算
6. 非group by模式下关联条件中的左表字段必须出现在主查询语句中比如: on t1.role_id=t2.id 其中的 t1.role_id 必须出现在主select语句中
7. 如果使用了group by,group by的字段不可为做任何计算,group 字段必须在查询字段中体现(简单字段)
8. 非group by模式必须设置主键，主键字段在目标表允许重复，主键字段必须设置在主表上，且不可被更新
9. 如果需要使用union all的话，建议在unionAll语句的各个分段中添加常量字段，然后每个分段分别添加相应的配置文件指向同一个目标表，并且配置deleteCondition(例如:常量字段在目标表的字段名=分段对应的常量值)
10. 使用group by 需要确保符合only_full_group_by
11. group by模式下，主表必须有字段体现在group中
12. 每张表最好都是用别名，每个字段需要标识自己来自于哪张表

# 执行逻辑：
## insert:
1. 无group
    1. sql语句中只有单一表&所有查询字段均为简单字段：目标表执行insert value(dml.data)
    2. 插入的表是主表：执行查询(原始sql注入where主键=dml.data.主键值) 查询结果插入目标表
    3. 非主表：遍历从表匹配表名(有同名表需要执行多次) 
        1. 目标表执行 delete where on字段(对应的查询字段) = dml.data.相关字段的值
        2. 执行查询(原始sql注入where on字段(前表的关联字段) = dml.data.相关字段的值) 查询结果插入目标表
2. 有group
    1. 在group中该表/该表关联的前表字段有字段涉及：
        1. 目标表执行 delete where group字段 = dml.data.相关字段的值
        2. 执行查询(原始sql注入where group字段 = dml.data.相关字段的值) 查询结果插入目标表
    2. 均无涉及
        1. 执行查询(原始sql注入where on字段(关联前表的字段) = dml.data.相关字段的值)
        2. 循环查询结果
            1. 目标表执行 delete where 所有group字段 = 查询结果
            2. 执行查询(原始sql注入where 所有group字段 = 查询结果) 查询结果插入目标表
    
## update：
1. 无group
    1. sql语句中只有单一表&所有查询字段均为简单字段：目标表执行update set where 主键=dml.data.主键的值
    2. 更新的表为主表(在查询字段中有引用)
        1. 目标表执行delete where 主键=dml.data.主键的值
        2. 执行查询(原始sql注入条件 主键=dml.data.主键的值)
    3. 非主表：遍历所有表匹配表名(有同名表需要执行多次)
        1. 更新的字段在 查询字段/关联前表的字段 中无引用：跳过
        2. 关联前表的字段没有发生更新
            1. 目标表执行 delete where on字段(对应的查询字段) = dml.data.相关字段的值
            2. 执行查询(原始sql注入where on字段(前表的关联字段) = dml.data.相关字段的值) 查询结果插入目标表
        3. 关联前表的字段发生了更新：
            1. 目标表执行 delete where on字段(对应的查询字段) = dml.data.相关字段的值
            2. 执行查询(原始sql注入where on字段(前表的关联字段) = dml.data.相关字段的值) 查询结果插入目标表
            3. 目标表执行 delete where on字段(对应的查询字段) = dml.old.相关字段的值
            4. 执行查询(sql注入where on字段(前表的关联字段) = dml.old.相关字段的值) 查询结果插入目标表
2. 有group
    1. 更新的字段在查询字段/关联前后表的字段中无引用：continue
    2. 在group中该表/该表关联的前表字段有字段涉及：
        1. 目标表执行 delete where group字段 = dml.data.相关字段的值
        2. 执行查询(原始sql注入where group字段 = dml.data.相关字段的值) 查询结果插入目标表
        3. 目标表执行 delete where group字段 = dml.old.相关字段的值
        4. 执行查询(原始sql注入where group字段 = dml.old.相关字段的值) 查询结果插入目标表
    3. 均无涉及：
        1. 执行查询(原始sql注入where on字段(关联前表的字段) = dml.data.相关字段的值)
        2. 循环查询结果
            1. 目标表执行 delete where 所有group字段 = 查询结果
            2. 执行查询(原始sql注入where 所有group字段 = 查询结果) 查询结果插入目标表
        3. 执行查询(原始sql注入where on字段(关联前表的字段) = dml.old.相关字段的值)
        4. 循环查询结果
            1. 目标表执行 delete where 所有group字段 = 查询结果
            2. 执行查询(原始sql注入where 所有group字段 = 查询结果) 查询结果插入目标表

## delete:
1. 无group
    1. 是主表：目标表执行delete where 主键=dml.data.主键的值
    2. 非主表：遍历从表匹配表名(有同名表需要执行多次)
        1. 目标表执行 delete where on字段(对应的查询字段) = dml.data.相关字段的值
        2. 执行查询(原始sql注入where on字段(前表的关联字段) = dml.data.相关字段的值) 查询结果插入目标表
2. 有group
    1. 在group中该表/该表关联的前表字段有字段涉及：
        1. 目标表执行 delete where group字段 = dml.data.相关字段的值
        2. 执行查询(原始sql注入where group字段 = dml.data.相关字段的值) 查询结果插入目标表
    2. 均无涉及
        1. 执行查询(原始sql注入where on字段(关联前表的字段) = dml.data.相关字段的值)
        2. 循环查询结果
            1. 目标表执行 delete where 所有group字段 = 查询结果
            2. 执行查询(原始sql注入where 所有group字段 = 查询结果) 查询结果插入目标表
        
        
        
# 其他
SqlParse类 移除【关联条件必须要有一个字段出现在主查询语句中】的限制，否则无法实现以下sql的数据同步
```sql
select t1.main_id, sum(t1.num*t2.price) total_amount
from t_detail t1
left join t_product t2
on t1.prod_code = t2.prod_code
group by t1.main_id;
```
不符合规范的表可以考虑提供监听相关表做清空重插