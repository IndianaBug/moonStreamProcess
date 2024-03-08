# kucoin get public token  POST /api/v1/bullet-public
# https://www.kucoin.com/docs/websocket/basic-info/ping
# Create a keep alive to send ping message

import json 
import requests
import time
import hashlib
import hmac
import base64
import websockets 
import asyncio
# from aiokafka import AIOKafkaProducer
# from aiokafka.errors import KafkaStorageError
import ssl

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


# Kucoin websocket
def obtain_kucoin_token_endpoint():
    """
        Returns kucoin token and endpoint
    """
    kucoin_api = "https://api.kucoin.com/api/v1/bullet-public"
    response = requests.post(kucoin_api)
    kucoin_token = response.json().get("data").get("token")
    kucoin_endpoint = response.json().get("data").get("instanceServers")[0].get("endpoint")
    return kucoin_token, kucoin_endpoint

kucoin_token, kucoin_endpoint = obtain_kucoin_token_endpoint()



kucoin_connectId = "hQvf8jkno21d65adaffs"  # Made it up
wskucoin = f"{kucoin_endpoint}?token={kucoin_token}&[connectId={kucoin_connectId}]"


# data =            {
#           "exchange":"kucoin", 
#           "instrument": "btcusdt", 
#           "insType":"spot", 
#           "obj":"depth", 
#           "updateSpeed" : 0, 
#           "url" : wskucoin,
#           "msg" : {
#                     "id": build_kucoin_wsendpoint(),   # generate random integer
#                     "type": "subscribe",
#                     "topic": "/market/level2:BTC-USDT",
#                     "response": True
#                     }
#         }

# data_trades =            {
#           "exchange":"kucoin", 
#           "instrument": "btcusdt", 
#           "insType":"spot", 
#           "obj":"trades", 
#           "updateSpeed" : 0, 
#           "url" : wskucoin,
#           "msg" : {
#                     "id": 1545910660740,   # generate random integer
#                     "type": "subscribe",
#                     "topic": "/market/match:BTC-USDT",
#                     "response": True
#                     }
#         }

# data =            {
#           "exchange":"gateio", 
#           "instrument": "btcusdt", 
#           "insType":"spot", 
#           "obj":"depth", 
#           "updateSpeed" : 0, 
#           "url" : "wss://api.gateio.ws/ws/v4/",
#           "msg" : {
#                     "id": build_kucoin_wsendpoint(),   # generate random integer
#                     "type": "subscribe",
#                     "topic": "/market/level2:BTC-USDT",
#                     "response": True
#                     }
#         }

data_trades =            {
          'exchange':'gateio', 
          'instrument': 'btcusdt', 
          'insType':'spot', 
          'obj':'trades', 
          'updateSpeed' : 0, 
          'url' : "wss://api.gateio.ws/ws/v4/",
          'msg' : {
                        "time": int(time.time()),
                        "channel": "spot.trades",
                        "event": "subscribe",  
                        "payload": ["BTC_USDT"]
                    }

        }


data_depth =            {
          'exchange':'gateio', 
          'instrument': 'btcusdt', 
          'insType':'spot', 
          'obj':'depth', 
          'updateSpeed' : 0, 
          'url' : "wss://api.gateio.ws/ws/v4/",
          'msg' : {
                        "time": int(time.time()),
                        "channel": "spot.order_book_update",
                        "event": "subscribe",  
                        "payload": ["BTC_USDT", "1000ms"]
                    }

        }




class btcproducer():

    def __init__ (self, host, data):
        self.host = host
        self.data = data

    async def keep_alive(self, websocket, exchange, ping_interval=30):
        while True:
            try:
                if exchange == "binance":
                    await websocket.pong()
                    await asyncio.sleep(ping_interval)
                if exchange == "okx":
                    await asyncio.sleep(ping_interval - 10)
                    await websocket.send('ping')
                if exchange == "bybit":
                    await asyncio.sleep(ping_interval - 10)
                    await websocket.send(json.dumps({"op": "ping"}))  
                if exchange == "coinbase":
                    await asyncio.sleep(ping_interval - 10)
                if exchange == "kucoin":
                    await asyncio.sleep(ping_interval - 10)
                    await websocket.send(json.dumps({"type": "ping", "id":1545910660740}))   # generate random id
                if exchange == "gateio":
                    await asyncio.sleep(ping_interval - 25)
                    await websocket.send("ping")
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed. Stopping keep-alive.")
                break
    
    async def receive_messages(self, websocket):
        try:
            async for message in websocket:
                print(message)
        except asyncio.exceptions.TimeoutError:
            print("WebSocket operation timed out")

    async def websocket_connection(self, connection_data):

        count = 1
        exchange = connection_data["exchange"]
        instrument = connection_data["instrument"]
        insType = connection_data["insType"]
        obj = connection_data["obj"]
        endpoint = connection_data["url"]
        msg = connection_data["msg"]


        async for websocket in websockets.connect(endpoint, ping_interval=30, timeout=86400): #, ssl=ssl_context, max_size=1024 * 1024 * 10):
                    
            await websocket.send(json.dumps(msg))
            
            keep_alive_task = asyncio.create_task(self.keep_alive(websocket, exchange, 30))
         
            try:
                if obj != "heartbeat":
                    async for message in websocket:
                        try:
                            print(message)
                        except:
                            continue
            except asyncio.exceptions.TimeoutError:
                print("WebSocket operation timed out")
                await asyncio.sleep(5)
                continue
            except websockets.exceptions.ConnectionClosed:
                print("connection  closed of bitget stream, reconnecting!!!")
                await asyncio.sleep(5)
                continue



    async def main(self):
        """
            Make sure to call btcusdt trades in the first place
        """
        #producer = AIOKafkaProducer(bootstrap_servers=self.host)
        #await producer.start()
        producer = ''
        topic = ''

        tasks = []
        tasks +=  [self.websocket_connection(self.data)]

        await asyncio.gather(*tasks) 


if __name__ == '__main__':
    client = btcproducer('localhost:9092', data_depth)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.main())

