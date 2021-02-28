import asyncio
import logging

from aiohttp import web

from opcuaconnectorpool import OPCUAConnectorPool

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('APIHandler_')


class APIHandler(object):

    def __init__(self):
        self.opcua_pool: OPCUAConnectorPool = OPCUAConnectorPool([])

    async def add_connector(self, request: web.Request):
        data = await request.json()
        server_configs = data['connector']
        res = await self.opcua_pool.setup(server_configs)
        return web.Response(text="%s OK" % res)

    async def remove_connector(self, request: web.Request):
        data = await request.json()
        uids = data['uids']
        res = await self.opcua_pool.remove_connectors(uids)
        return web.Response(text="OK\n")

    async def shutdown(self, _):
        await self.opcua_pool.shutdown()
