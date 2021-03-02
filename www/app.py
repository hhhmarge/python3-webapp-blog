#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'wt'

'web app 的骨架'

import logging
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web

logging.basicConfig(level=logging.INFO)


async def index(request):
    return web.Response(body=b'<h1>Index</h1>', headers={'content-type': 'text/html'})


# 初始化函数，是一个协程
def init():
    app = web.Application()
    app.router.add_get('/', index)
    # aiohttp 规格改变：http://demos.aiohttp.org/en/latest/tutorial.html
    # web.run_app()内部调用了 loop 和 logging
    web.run_app(app, host='127.0.0.1', port=9000)


if __name__ == '__main__':
    init()
'''
# 教程的源码，需要更新
async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)  # 参数分别是 method，path，handler
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)  # 创建一个 TCP 服务器，规定 host 和 port
    logging.info('Server started at http://127.0.0.1:9000...')
    return srv


loop = asyncio.get_event_loop()  # Return an asyncio event loop
loop.run_until_complete(init(loop))  # Run the event loop until a Future is done
loop.run_forever()
'''
