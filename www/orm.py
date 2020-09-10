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
    async with __connection_pool,get as conn:
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
