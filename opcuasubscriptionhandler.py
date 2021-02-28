import asyncio
import os
import pathlib
import shutil
import uuid

from asyncua.ua import DataChangeNotification


class OPCUASubHandler(object):
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """

    def __init__(self):
        self.setup_files()

    def setup_files(self):
        dir = pathlib.Path(__file__).parent.absolute()
        if not os.path.exists(os.path.join(dir, "files")):
            os.mkdir(os.path.join(dir, "files"))
        else:
            shutil.rmtree(os.path.join(dir, "files"))

    def datachange_notification(self, node, val, data):
        task = asyncio.create_task(self.persist_datachange(node, val, data))
        task.add_done_callback(self.persisted_callback)

    def event_notification(self, event):
        print("New event", event)

    async def persist_datachange(self, node, val, data: DataChangeNotification):
        fileid = uuid.uuid4()
        v = data.monitored_item.Value
        print(node, val, v.SourceTimestamp, v.ServerTimestamp)
        # filename = "/Users/pranav/workspace/opcua_toy/files/node_%s.csv" % fileid
        # with open(filename, 'w') as file:
        #     file.write("node,server_ts,source_ts,val\n")
        #
        #     # print(dir(v))
        #     file.write("%s,%s,%s,%s" % (node, v.ServerTimestamp, v.SourceTimestamp, val))

    def persisted_callback(self, t):
        pass
