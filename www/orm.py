import logging,aiomysql

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


# 定义ORM
#1、定义所有ORM映射的基类Model
class Model(dict):
    def __init__(self, **kwargs):
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

#2、各类数据Field
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

