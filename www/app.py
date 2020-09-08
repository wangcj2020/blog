import logging; logging.basicConfig(level=logging.INFO)

import asyncio

from aiohttp import web

def index(request):
    # 访问"/"时的响应
    return web.Response(body='<h1>Blog</h1>',content_type="text/html")

# 协程，异步
async def init(loop):
    app = web.Application()
    app.router.add_route("GET","/",index)


    runner = web.AppRunner(app=app)
    await runner.setup()
    site = web.TCPSite(runner,"127.0.0.1",9000)
    logging.info('server started at http://127.0.0.1:9000...')
    await site.start()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()