import concurrent
import functools
import logging

import sys
from asyncio import Task, futures, AbstractEventLoop
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, List

import asyncio

from opcuaconnector import OPCUAConnector


class OPCUAConnectorPool(object):

    MAX_WORKERS = 5

    def __init__(self, server_configs: list):
        self._server_configs = server_configs
        self._pool: Dict[int, OPCUAConnector] = dict()
        self._tasks: Dict[int, Task] = dict()
        self._execpool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=self.MAX_WORKERS,
                                                                thread_name_prefix="opcua_")
        # self._loop.run_forever()

    async def setup(self, server_configs=None):
        cnf = self._server_configs if server_configs is None else server_configs
        for s in cnf:
            loop = asyncio.get_running_loop()
            # asyncio.create_subprocess_exec(sys.executable, )

            self._execpool.submit(
                lambda x: asyncio.create_task(self.add_connector(s['url'], s['node_ids'], s['uid']))
            )

    async def add_connector(self, url, node_ids, uid):
        connector = OPCUAConnector(url, node_ids, uid)
        if await connector.connect():
            t = asyncio.create_task(connector.subscribe())
            self._tasks[connector.uid] = t
            self._pool[connector.uid] = connector
            logging.info("Added Connector with ID: %d to pool" % connector.uid)
            return connector.uid
        else:
            logging.error("Unable to connect. Quitting.")

    async def remove_connectors(self, uids: List[int]) -> None:
        [asyncio.create_task(self.remove_connector(x)) for x in uids]

    async def remove_connector(self, uid) -> None:
        c: OPCUAConnector = self._pool.get(uid)
        if c:
            task = asyncio.create_task(c.disconnect())
            task.add_done_callback(functools.partial(self._remove_from_pool, uid))
        else:
            logging.warning(self._pool.keys())

    def _remove_from_pool(self, uid, _):
        self._pool.pop(uid)
        asyncio.gather(self._tasks.pop(uid))
        logging.info("Removed %d from Pool" % uid)

    async def shutdown(self):
        logging.info("Starting Shutdown of OPCUAConnectorPool.")
        print(self._pool.keys())
        for uid, connector in self._pool.items():
            t: Task = asyncio.create_task(connector.disconnect())
            t.add_done_callback(functools.partial(self._remove_from_pool, uid))

