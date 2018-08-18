
import asyncio, logging

import aiomysql

def log(sql, args=()):
  logging.info('SQL: %s' % sql)

# 创建连接池
async def crete_pool(loop, **kw):
  logging.info('create database connection pool...')
  global __pool
  __pool = await aiomysql.create_pool(
    host=kw.get('host', 'localhost'),
    port=kw.get('port', 3306),
    user=kw['root'],
    password=kw['666'],
    db=kw['db'],
    charset=kw.get('charset', 'utf8'),
    autocommit=kw.get('autocommit', True), # 自动提交事务
    maxsize=kw.get('maxsize', 10),
    minsize=kw.get('minsize', 1),
    loop=loop
  )
async def select(sql, args, size=None):
  log(sql, args)
  global __pool
  async with __pool.get() as conn:
    async with conn.cursor(aiomysql.DictCursor) as cur:
      await cur.execute(sql.replace('?', '%s'), args or ())
      if size:
        rs = await cur.fetchmany(size)
      else:
        rs = await cur.fetchall()
    logging.info('rows return: %s' % len(rs))
    return rs
