import asyncio
import functools
import json
import logging
import sys
from asyncio import Task
from asyncio.subprocess import Process
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('OpcUAConnectorPool_')


class OPCUAConnectorPool(object):

    def __init__(self, server_configs: list):
        self._server_configs = server_configs
        self._process_pools: Dict[int, Process] = dict()

    async def setup(self, server_configs=None):
        cnf = self._server_configs if server_configs is None else server_configs
        for c in cnf:
            asyncio.create_task(self.add_connector([c]))

    async def add_connector(self, server_configs=None):
        assert len(server_configs) == 1
        config = json.dumps(server_configs)
        _logger.info(config)
        uid = server_configs[0]['uid']
        task = asyncio.create_task(asyncio.create_subprocess_exec(sys.executable,
                                                                  '/Users/pranav/workspace/opcua_toy/opcuaconnector.py',
                                                                  config))
        task.add_done_callback(functools.partial(self.task_created, task, uid))

    def task_created(self, task: Task, uid: int, x):
        proc = task.result()
        _logger.info("%s Created for connector ID %d" % (proc, uid))
        self._process_pools[uid] = proc

    async def remove_connectors(self, uids: List[int]) -> None:
        _logger.info("Shutting down processes for Connector IDs %s" % ','.join(map(str, uids)))
        [self._process_pools.pop(x).kill() for x in uids]

    async def shutdown(self):
        logging.info("Starting Shutdown of OPCUAConnectorPool.")
        await self.remove_connectors(list(self._process_pools.keys()))


async def main(server_configs):
    pool = OPCUAConnectorPool(server_configs)
    asyncio.create_task(pool.setup(server_configs))


if __name__ == '__main__':
    server_configs = [
        {"url": "opc.tcp://Pranavs-MacBook-Pro-2.local:53530/OPCUA/SimulationServer",
         "node_ids": ["ns=3;i=1001", "ns=3;i=1011"],
         "uid": 1
         },
        {"url": "opc.tcp://Pranavs-MacBook-Pro-2.local:53530/OPCUA/SimulationServer",
         "node_ids": ["ns=3;i=1022", "ns=3;i=1004"],
         "uid": 2
         }
    ]
    loop = asyncio.get_event_loop()
    try:
        main_task = loop.create_task(main(server_configs))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
