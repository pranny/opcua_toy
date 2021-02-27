import asyncio
import logging
from aiohttp import web

from api_handler import APIHandler

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncuatoy')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    handler = APIHandler()
    app = web.Application()
    app.add_routes([
        web.post('/connector', handler.add_connector),
        web.post('/remove_connector', handler.remove_connector)
    ])
    app.on_shutdown.append(handler.shutdown)
    web.run_app(app)
