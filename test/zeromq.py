import asyncio

import zmq.asyncio
import zmq
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())




class App:
    def __init__(self, loop, link):
        self.loop = loop
        self.link = link
        asyncio.ensure_future(self.start(), loop=self.loop)

    async def start(self):
        self.zmqContext = zmq.asyncio.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        self.zmqSubSocket.connect(self.link)
        self.loop.create_task(self.zeromq_handler())

    async def zeromq_handler(self):
        print("zero mq handler started", self.link)
        while True:
            try:
                msg = await self.zmqSubSocket.recv_multipart()
                topic = msg[0]
                body = msg[1]
                if topic == b"rawtx":
                    print(body)
            except asyncio.CancelledError:
                self.log.warning("zeromq handler terminated")
                break
            except Exception as err:
                self.log.error(str(err))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = App(loop, "tcp://btc.three.bitaps.com:18900")
    loop.run_forever()
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
    loop.close()


