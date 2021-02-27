import functools
import logging
import pathlib
import shutil
import signal
import uuid
import os

import asyncio
from asyncio import Task
from opcuasubscriptionhandler import OPCUASubHandler

from asyncua import Client, ua, Node
from asyncua.common.subscription import Subscription, SubHandler
from asyncua.ua.uaprotocol_auto import DataChangeNotification

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('OpcUAConnector_')


class OPCUAConnector(object):

    __captured_signals__ = [signal.SIGTERM, signal.SIGABRT, signal.SIGHUP, signal.SIGQUIT, signal.SIGINT]

    def __init__(self, connection_string: str, node_ids: list, uid: int, publish_interval_requested: int = 10,
                 loop=None):

        if uid is None:
            raise ValueError("UID Cant be None")

        self.connection_string: str = connection_string
        self.node_ids: list = node_ids
        self.uid: int = uid
        self.publishing_interval_requested: int = publish_interval_requested

        self._opc_client: Client = Client(self.connection_string, loop=loop)
        self._opc_client.set_user("pp")
        self._opc_client.set_password("pp")

        self._opc_client_connected: bool = False
        self._continue_forever: bool = True

    @property
    def nodes(self):
        return [Node(self._opc_client.uaclient, x) for x in self.node_ids]

    async def connect(self) -> bool:

        try:
            await self._opc_client.connect()
            self._opc_client_connected = True

        except ConnectionRefusedError as exception:
            _logger.info("Timeout while connecting. Is the server running?")
            _logger.exception(exception)

        except asyncio.futures.TimeoutError as e:
            _logger.info("Timeout while connecting. Is the server running?")
            # logging.exception(e)

        except Exception as e:
            _logger.exception(e)

        finally:
            return self._opc_client_connected

    async def disconnect(self):
        _logger.info("Disconnecting: %d" % self.uid)
        try:
            await self._opc_client.disconnect()
            _logger.info("Disconnected: %d" % self.uid)
            return True
        except Exception as e:
            logging.exception(e)
            return False

    async def subscribe(self):
        await self._subscribe()
        try:
            while True:
                await asyncio.sleep(0)
        except asyncio.CancelledError as e:
            _logger.info("Task Cancelled")
            await self.disconnect()

    async def _subscribe(self):
        handler_params = ua.CreateSubscriptionParameters()
        handler_params.RequestedPublishingInterval = self.publishing_interval_requested
        handler_params.RequestedLifetimeCount = 10000
        handler_params.RequestedMaxKeepAliveCount = self._opc_client.get_keepalive_count(
            self.publishing_interval_requested)
        handler_params.MaxNotificationsPerPublish = 10000
        handler_params.PublishingEnabled = True
        handler_params.Priority = 0

        handler: OPCUASubHandler = OPCUASubHandler()

        subscription: Subscription = await self._opc_client.create_subscription(handler_params, handler,
                                                                                publishing=True)
        await subscription.subscribe_data_change(self.nodes)


async def main(server_configs):
    for c in server_configs:
        connector = OPCUAConnector(c['url'], c['node_ids'], c['uid'])
        if await connector.connect():
            await connector.subscribe()
        else:
            _logger.info("Unable to connect")


if __name__ == '__main__':
    server_configs = [
        {"url": "opc.tcp://Pranavs-MacBook-Pro-2.local:53530/OPCUA/SimulationServer",
         "node_ids": ["ns=3;i=1001", "ns=3;i=1011"],
         "uid": 1
         }
    ]
    _logger.info("Starting")
    asyncio.run(main(server_configs))



