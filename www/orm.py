#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'wt'
'编写 ORM（数据库和实体类的映射），并封装常用的查询操作'

import aiomysql, logging, asyncio


# 创建基本日志函数
def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 创建连接池，复用数据库连接：https://aiomysql.readthedocs.io/en/latest/pool.html?highlight=create_pool
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    # 连接池由全局变量__pool 储存，缺省情况下将编码设置为utf8，自动提交事务
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


# 封装 select 操作：https://aiomysql.readthedocs.io/en/latest/cursors.html
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)  # 要求返回 dict 格式
        # cur 可以单独执行 sql 语句，sql 的占位符是？，MySQL 的占位符是 %s
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)  # 获取最多指定数量的记录，返回一个列表
        else:
            rs = await cur.fetchall()  # 获取所有记录
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        # 返回了一个列表
        return rs


# 用一个通用函数封装 insert，update，delete 操作，因为参数和返回值相同
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount  # 返回受影响的行数
            await cur.close()
        except BaseException as e:
            raise
        return affected


# 实现 ORM
# 在构造默认的INSERT, UPDATE和DELETE语句时，生成 sql 语句的占位符，如[?,?,?,?]
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# ① 定义 Field 类及其子类，负责保存数据库表的字段名和字段类型
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):  # 映射 varchar 的 StringField

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)  # super 在单继承中用来引用父类而不必显式地指定它们的名称


class BooleanField(Field):  # 映射 boolean 的 BooleanField

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):  # 映射 bigint 的 IntegerField

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):  # 映射 real 的 FloatField

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):  # 映射 text 的 TextField

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# ② 定义元类，动态修改 Model基类，读取具体子类的映射信息
# 继承 Model 基类的子类，会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性如__table__、__mappings__中。
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):  # 参数：__new__(准备创建类的对象，类的名称，类继承的父类集合，类方法集合)
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():  # k是子类属性，v 是数据库字段名
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)  # 由子类属性组成的 list
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():  # 如果 key 存在于字典中则将其移除并返回其值，否则返回 default
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))  # 给 fields 加上格式: ['`field1`', '`field2`']
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)),
            primaryKey)  # 先从数据库字段里找，没有的话看子类属性
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# ③ 定义所有ORM映射的基类Model
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 返回参数为 key 的自身属性
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    # 返回属性的值
    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # 定位 key
            field = self.__mappings__[key]
            if field.default is not None:  # 返回默认值
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 往 Model 类添加 class 方法，就可以让所有子类调用 class 方法，不需要实例化（即不需要传入 self 参数）
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        # where 默认值为 None，如果 where 有值就在 sql 加上字符串 'where' 和 变量 where
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        # get 可以返回 orderBy 的值，如果失败就返回 None ，这样失败也不会出错
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)  # extend 把 iterable的元素挨个加到末尾
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]  # 返回查询出的所有列表

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. 适用于 select count(*)类型的 sql '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # 往 Model 类添加实例方法，就可以让所有子类调用实例方法
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):  # delete by primary key
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)
