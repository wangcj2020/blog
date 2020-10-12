import asyncio
import logging
import random
import sys

logging.basicConfig(level=logging.INFO)
import aiomysql

def log(sql, args=()):
    '''
    sql语句日志信息
    :param sql:
    :param args:
    :return:
    '''
    logging.info('SQL：%s' % sql)
    logging.info('ARGS：%s' % (''+args))

async def create_pool(loop,**kwargs):
    '''
    创建线程池（异步）
    :param loop:
    :param kwargs:
    :return:
    '''
    logging.info("create a database connection pool...")
    global __connection_pool
    __connection_pool = await aiomysql.create_pool(
        host=kwargs.get("host","localhost"),
        post=kwargs.get("port",3306),
        user=kwargs['user'],
        password=kwargs['password'],
        db=kwargs['db'],
        charset=kwargs.get("charset","utf-8"),
        autocommit=kwargs.get("autocommit",True),
        maxsize=kwargs.get("maxsize",10),
        minsize=kwargs.get("minsize",1),
        loop=loop
    )

async def select(sql, args, size=None):
    '''
    数据查询
    :param sql:
    :param args:
    :param size:
    :return:
    '''
    log(sql, args)
    global __connection_pool
    async with __connection_pool.get() as conn: #从线程池中获取一个连接
        async with conn.cursor(aiomysql.DictCursor) as cur: # 获取游标
            await cur.execute(sql.replace('?','%s'),args or ())
            if size:# 获取size条数据
                rs = await cur.fetchmany(size)
            else:# 获取所有数据
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args, autocommit=True):
    '''
    数据增、删、改时的执行函数
    :param sql:
    :param args:
    :param autocommit: 是否设置了自动提交
    :return:
    '''
    log(sql,args)
    global __connection_pool
    async with __connection_pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affect_row_count = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affect_row_count

# 查询字段计数：替换成sql识别的'？'(生成'?,?,?,?,?')
def create_args_string(num):
    list_0 = []
    for i in range(num):
        list_0.append('?')
    return ','.join(list_0)

#3、定义信息读取类ModelMetaclass
class ModelMetaclass(type):
    '''
    创建一个type类型的对象
    '''
    # cls：代表要__init__的类，此参数在实例化时由python解释器自动提供（eg：下文的User、Model)
    # bases:父类的集合
    # attrs:类中所有内容(属性及方法)
    # cls、bases、attrs又解释器自动传入
    # name:类名
    def __new__(cls, name, bases, attrs):
        print("ModelMetaclass new!")
        # print("cls=%s" % cls)
        # print("name=%s" % name)
        # print("bases=",bases)
        # print("attrs=",attrs)
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)

        # 获取表信息
        # attrs中没有时默认表名为类名name
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (tableName:%s)' % (name, tableName))
        # 获取所有的Field和主键
        mappings = dict() # 类属性与字段间的对应关系
        fields = [] # 保存非主键属性名
        primaryKey = None
        # k:类的属性(字段名)；v：数据库表中对应的Field属性
        for k,v in attrs.items():
            # 找到类属性中的字段属性
            if isinstance(v,Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 判断v是否是主键，从而决定k是否放入fields中
                if v.is_primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)

        if not primaryKey:
            raise RuntimeError('Primary key not found.')

        for k in mappings.keys():
            attrs.pop(k)

        # 将非主键属性变成`id`,`name`这种形式（带反引号）
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        # 添加默认的增删改查语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (
            primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(
            escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(
            map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (
            tableName, primaryKey)
        # print(attrs['__select__'])
        # print(attrs['__insert__'])
        # print(attrs['__update__'])
        # print(attrs['__delete__'])
        return type.__new__(cls, name, bases, attrs)


# 定义ORM
#1、定义所有ORM映射的基类Model,(User等对象可以继承该类)
class Model(dict,metaclass=ModelMetaclass):
    def __init__(self, **kwargs):
        print("Model init!")
        print("kwargs=",kwargs)
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        # 没有取到值，获取默认值
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # 数据库操作方法
    @classmethod
    # 申明是类方法：有类变量cls传入，cls可以做一些相关的处理
    # 有子类继承时，调用该方法，传入的类变量cls是子类，而非父类
    async def find_all(cls, where=None, args=None, **kw):
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
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
                sql.append('?,?')
                args.append(limit)
            else:
                raise ValueError('invalid limit value:%s' % str(limit))
        # 返回的rs是一个元素是tuple的list
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]
        # **r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例
    #
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        '''find number by select and where'''
        sql = ['select %s __num__ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            args.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    @classmethod
    async def find(cls, primaryKey):
        '''find object by primary key'''
        # rs是一个list，里面是一个dict
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [primaryKey], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
        # 返回一条记录，以dict的形式返回，因为cls的父类继承了dict类

    # 根据条件查找
    @classmethod
    async def findAll(cls, **kw):
        rs = []
        if len(kw) == 0:
            rs = await select(cls.__select__, None)
        else:
            args = []
            values = []
            for k, v in kw.items():
                args.append('%s = ?' % k)
                values.append(v)
            print('%s where %s' % (cls.__select__, ' and '.join(args)), values)
            rs = await select('%s where %s' % (cls.__select__, ' and '.join(args)), values)
        return rs

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.info('failed to insert record:affected rows:%s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.info('failed to update record:affected rows:%s' % rows)

    async def delete(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.info('failed to delete by primary key:affected rows:%s' % rows)

#2、各类数据Field，定义数据库中各个字段的名和类型
class Field(object):
    def __init__(self, name, col_tpye, is_primary_key, default_value):
        self.name = name
        self.col_type = col_tpye
        self.is_primary_key = is_primary_key
        self.default_value = default_value

    def __str__(self):
        return '%s, %s:%s' % (self.__class__.__name__, self.col_type, self.name)

class StringField(Field):
    def __init__(self, name=None, col_type='varchar(100)',is_primary_key=False, default_value = None):
        super().__init__(name, col_type, is_primary_key, default_value)

class BooleanField(Field):
    def __init__(self, name=None, default_value=False):
        super().__init__(name, 'boolean', False, default_value)

class TextField(Field):
    def __init__(self, name=None, default_value=None):
        super().__init__(name, 'text', False, default_value)

class IntegerField(Field):
    def __init__(self, name=None, is_primary_key=False, default_value=0):
        super().__init__(name, 'int', is_primary_key, default_value)

class FloatField(Field):
    def __init__(self, name=None, is_primary_key=False, default_value=0.0):
        super().__init__(name, 'float', is_primary_key, default_value)


if __name__ == "__main__":
    class User(Model):
        # 定义类的属性到列的映射：
        id = IntegerField('id', is_primary_key=True)
        name = StringField('name')
        email = StringField('email')
        password = StringField('password')

    #创建异步事件的句柄
    loop = asyncio.get_event_loop()

    #创建实例
    async def test():
        # await create_pool(loop=loop,host='localhost',port=3306,user='root',password='1996',db='test')#单引号表示空格
        user = User(id = random.randint(5,100),name='xh',email='xh@pthon.com',password='123456')
        # await user.save() #插入一条记录：测试insert
        # print(user)
        # #这里可以使用User.findAll()是因为：用@classmethod修饰了Model类里面的findAll()
        # #一般来说，要使用某个类的方法，需要先实例化一个对象再调用方法
        # #而使用@staticmethod或@classmethod，就可以不需要实例化，直接类名.方法名()来调用
        # r = await User.findAll(name='xh') #查询所有记录：测试按条件查询
        # print(r)
        # user1 = User(id = 2,name='xiong',email='xh@qq.com',password='123456') #user1是数据库中id已经存在的一行的新数据
        # u = await user1.update() #测试update,传入User实例对象的新数据
        # print(user1)
        # d = await user.delete() #测试delete
        # print(d)
        # s = await User.find(1) #测试find by primary key
        # print(s)
        # await destory_pool() #关闭数据库连接池

    loop.run_until_complete(test())
    loop.close()
    if loop.is_closed():
        sys.exit(0)
