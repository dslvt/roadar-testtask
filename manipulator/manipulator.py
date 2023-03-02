import asyncio
import websockets
import logging

log = logging.getLogger(__name__)

async def connect():
    async with websockets.connect("ws://controller:8000/manipulator/") as ws:
        print('Connected to server')
        while True:
            response = await ws.recv()
            print(response)

if __name__ == "__main__":
    print('started manipulator')
    asyncio.run(connect())