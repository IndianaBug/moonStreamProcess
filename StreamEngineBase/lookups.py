import json
import numpy as np
import datetime
from dateutil import parser
import arrow
from typing import Tuple
from utilis import calculate_option_time_to_expire_deribit, calculate_option_time_to_expire_okex, calculate_option_time_to_expire_bybit



unit_conversion_btc = {
    # Any call related to the instrumet
    "binance_perp_btcusd" : lambda value, btc_price: value * 100 / btc_price,  # https://www.binance.com/en/support/faq/binance-coin-margined-futures-contract-specifications-a4470430e3164c13932be8967961aede
    # "binance_perp_btcusd" : lambda x : x         # base asset                # https://www.binance.com/en/support/faq/usd%E2%93%A2-margined-futures-contract-specifications-360033161972     
    # "bybit_perp_btcusdt" : "",                   # base asset                # https://www.bybit.com/en/announcement-info/contract-detail/
    # Used to fetch the first depth via API and for OI websocket
    "bybit_perp_btcusd" :   lambda value, btc_price: value  / btc_price,       # https://www.bybit.com/data/basic/inverse/contract-detail?symbol=BTCUSD
    # Only for open interest websocket
    "bybit_perp_btcusdt" :   lambda value, btc_price: value  / btc_price,
    # For perp depth api (since the exchange doesnt provide a well structured depth websockets)
    "bingx_perp_btcusdt_depth" : lambda size: size / 10000,  # Not specified but was deduced from API #  https://bingx.com/en-us/tradeInfo/perpetual/contract-rules/BTC-USDT/
    "bingx_perp_btcusdt_OI" : lambda openInterest, price : openInterest / price,    # Not specified, deduced from coinmarcetcap and coinglass
    # bitget_btcusdt_perp is in btc units Deduced from coinmarcatcap and coinglass
    "deribit_perp_btcusd" : lambda size, price : size / price,                  # https://docs.deribit.com/#user-mmp_trigger-index_name
                                                                               # https://docs.deribit.com/#trades-instrument_name-interval
    # its seem the api call has some extra multiplier. Verified bt https://metrics.deribit.com/futures/BTC
    "deribit_perp_btcusd_OI" : lambda size, price : size / price / 10,          # https://docs.deribit.com/#public-get_book_summary_by_instrument
    # Depth and trades of perp were called via api since by the time of coding the websocket was broken
    # No specification but deduced with coinmarketcap
    "gateio_perp_btcusdt" : lambda size, price: size / price,     # https://www.gate.io/docs/developers/apiv4/en/#futures-order-book 
    "htx_perp_btcusdt" : lambda size, price :  size * 100 / price,
    # Deduced using coinmarketcap. Order book size is small for htx supporting the assumption that the total oi is small
    "htx_perp_btcusdt_OI" : lambda size, price:  size  / price,   
    # All option ois are in BTC units
    "kucoin_perp_btcusdt" : lambda size : size / 1000,  # https://www.kucoin.com/futures/contract/detail/XBTUSDTM # confirmed by using kucoin trade station
    # NOT sure, using their contract specification is clear that coinmarcetcap has wrong numbers. Their API support has been trash
    # BUT I think its better to make it smaller rather than bigget.
    # This one is almost 4 times smaller than the one from coinmarketcap
    "mexc_perp_btcusdt" : lambda size : size * 0.0001,     # https://futures.mexc.com/information/trading_rule
    # Applicable to trades and depth
    "okx_perp_btcusd" : lambda size, price : size /price * 100    # https://www.okx.com/help/i-perpetual-swaps
}

class btc():

    def __init__(self, unit_conversion_dict : dict):

        """
            unit_conversion_dict must contain a dictionary with functions to transform contracts units into btc units if necessary
        """
        self.unit_conversion_dict = unit_conversion_dict

    ### BINANCE ###

    def binance_liquidations_lookup(self, response : json) -> list:
        """
            [[side, price, amount timestamp]]   str, float, float, str
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        instrument = response["instrument"]
        try:
            l = []
            amount = float(response.get("data").get("o").get("q"))
            side = response.get("data").get("o").get("S").lower()
            price = float(response.get("data").get("o").get("p"))
            timestamp = response.get("data").get("o").get("T")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            if instrument == "btcusd":
                amount = self.unit_conversion_dict.get("binance_perp_btcusd")(amount, price)
            l.append([side, price, amount, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def binance_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data")[0].get("fundingRate"))
            timestamp = response.get("data")[0].get("fundingTime")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return funding, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def binance_OI_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            oi, price, timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        price = response.get("btc_price")
        try:
            openInterest = float(response.get("data").get("openInterest"))
            timestamp = response.get("data").get("time")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            if instrument == "btcusd":
                openInterest = self.unit_conversion_dict.get("binance_perp_btcusd")(openInterest, price)
            return openInterest, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def binance_GTA_TTA_TTP_lookup(self, response : json) -> Tuple[float, float, float, str]:
        """
            longAccoumt, shortAccount, longShortration, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            longAccount = float(response.get("data")[0].get("longAccount"))
            shortAccount = float(response.get("data")[0].get("shortAccount"))
            longShortRation = float(response.get("data")[0].get("longShortRatio"))
            timestamp = response.get("data")[0].get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return longAccount, shortAccount, longShortRation, price, timestamp
        except:
            try: 
                longAccount = float(response.get("data")[0].get("longPosition"))
                shortAccount = float(response.get("data")[0].get("shortPosition"))
                longShortRation = float(response.get("data")[0].get("longShortRatio"))
                timestamp = response.get("data")[0].get("timestamp")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return longAccount, shortAccount, longShortRation, price, timestamp
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None


    def binance_trades_lookup(self, response : json) -> list:
        """
            [side, price, amount, timestamp]
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response.get("btc_price")
        try:
            quantity = float(response.get("data").get("q"))
            price = float(response.get("data").get("p"))
            if response.get("data").get("m") == True:
                side = "buy"
            else:
                side = "sell"
            timestamp = response.get("data").get("E")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            if insType == "perpetual" and instrument == "btcusd":
                quantity = self.unit_conversion_dict.get("binance_perp_btcusd")(quantity, price)
            return [[side, price, quantity, timestamp]]
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def binance_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        side = "bid" if side == "bids" else "ask"
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        try:
            if side == "ask":
                for key in ["a", "asks"]:
                    books = response["data"].get(key, np.nan)
                    if books is np.nan:
                        continue
                    else:
                        break

            if side == "bid":
                for key in ["b", "bids"]:
                    books = response["data"].get(key, np.nan)
                    if books is np.nan:
                        continue
                    else:
                        break

            if response["data"].get("E", np.nan) != np.nan:
                try:
                    timestamp = datetime.datetime.fromtimestamp(float(response["data"].get("E")/ 10**3)).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            books = [[float(x[0]), float(x[1])] for x in books]

            if insType == "perpetual" and instrument == "btcusd":
                books = [[x[0], self.unit_conversion_dict.get("binance_perp_btcusd")(x[1], price)] for x in books]  
            
            return books, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None




    ### Bybit ###

    def bybit_GTA_lookup(self, response : json) -> Tuple[float, float, float, str]:
        """
            buyRation, sellRation, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            buyRation = float(response.get("data").get("result").get("list")[0].get("buyRatio"))
            sellRation = float(response.get("data").get("result").get("list")[0].get("sellRatio"))
            timestamp = int(response.get("data").get("result").get("list")[0].get("timestamp"))
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return buyRation, sellRation, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None



    def bybit_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """ 
            side : "bids" for books
                "asks" for asks
                returns: [[price, amount]...], timestamp
        """
        side = "a" if side == "asks" else "b"
        response = json.loads(response)
        instrument = response.get("instrument")
        insType = response.get("insType")
        price = response.get("btc_price")
        try:
            try:
                books = response["data"]["data"]
            except:
                books = response["data"]["result"]
            books = books.get(side)
            try:
                timestamp = float(response.get("data").get("ts"))
            except:
                timestamp = float(response.get("data").get("result").get("ts"))
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            if insType == "perpetual" and instrument == "btcusd" and response.get("data").get("topic") != "orderbook.200.BTCPERP":
                books = [[float(x[0]), self.unit_conversion_dict.get("bybit_perp_btcusd")(float(x[1]), price)] for x in books]
            else:
                books = [[float(x[0]), float(x[1])] for x in books]
            return books, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def bybit_OI_funding_lookup(self, response : json) -> Tuple[float, float, float, str]:
        """
            fundingRatem OI price, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            funding = float(response.get("data").get("data").get("fundingRate"))
            timestamp = float(response.get("data").get("ts"))
            openInterestValue = float(response.get("data").get("data").get("openInterestValue"))
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            openInterestValue = self.unit_conversion_dict.get("bybit_perp_btcusdt")(openInterestValue, price)
            return funding, openInterestValue, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def bybit_liquidations_lookup(self, response : json) -> list:
        """
            [[side, price, size, timestamp]]
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            l = []
            side = response.get("data").get("data").get("side", np.nan).lower()
            price = float(response.get("data").get("data").get("price", np.nan))
            size = float(response.get("data").get("data").get("size", np.nan))
            timestamp = response.get("data").get("data").get("updatedTime", np.nan)
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            l.append([side, price, size, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None



    def bybit_trades_lookup(self, response : json) -> list:
        """
            [[side, price, size, timestamp]]
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            l = []
            for trade in response.get("data").get("data"):
                side = trade.get("S").lower()
                price = float(trade.get("p"))
                size = float(trade.get("v"))
                timestamp = trade.get("T")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, size, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def bybit_option_oi_lookup(self, response : json, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
        """
            side : P, C
            returns: strikes, countdowns, ois, price, timestamp
        """

        r = json.loads(response)
        price = r["btc_price"]
        try:
            strikes = np.array([float(d.get("symbol").split("-")[-2]) for d in r.get("data").get("result").get("list") if d.get("symbol").split("-")[-1] == side])
            ois = np.array([float(d.get("openInterest")) for d in r.get("data").get("result").get("list") if d.get("symbol").split("-")[-1] == side])
            countdowns = np.array([calculate_option_time_to_expire_bybit(d.get("symbol").split("-")[1]) for d in r.get("data").get("result").get("list") if d.get("symbol").split("-")[-1] == side])
            
            timestamp = datetime.datetime.fromtimestamp(r.get("data").get("time") / 10**3).strftime('%Y-%m-%d %H:%M:%S')
            
            return strikes, countdowns, ois, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None




    ### COINBASE ###

    def coinbase_depth_lookup(self, response, side) -> Tuple[list, str]:
        """
            side : asks, bids
            returns: returns: [[price, amount]...], timestamp
        """
        side = "offer" if side == "asks" else "bid"

        response = json.loads(response)
        # The first response may be a subscription info
        if response.get("data").get("events", np.nan) != np.nan:
            try: 
                if response.get("data").get("events")[0].get("subscriptions", None) != None:  
                    return None
            except:
                pass

        # Websockets stream
        if response.get("data").get("events", np.nan) != np.nan:
            try:
                if response.get("data").get("events")[0].get("updates", None) != None:
                    books = response.get("data").get("events")[0].get("updates")
                    formated_books = []
                    for book in books:
                        if book.get("side") == side:
                            formated_books.append([float(book.get("price_level")), float(book.get("new_quantity"))])
                    timestamp = parser.parse(response.get("data").get("events")[0].get("updates")[0].get("event_time")).strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass

        # API snapshot
        if response.get("data").get("pricebook", None) != None:
            if side == "bid":
                side = "bids"
            if side == "offer":
                side = "asks"
            books = response.get("data").get("pricebook").get(side)
            formated_books = []
            for book in books:
                formated_books.append([float(book.get("price")), float(book.get("size"))])
            timestamp = parser.parse(response.get("data").get("pricebook").get("time")).strftime('%Y-%m-%d %H:%M:%S')
        
        return formated_books, timestamp



    def coinbase_trades_lookup(self, response : dict) -> list:
        """
            [side, price, size, timestamp]
        """
        response = json.loads(response)
        try:
            l = []
            trades = response.get("data").get("events")[0].get("trades")
            for trade in trades:
                side = trade.get("side").lower()
                price = float(trade.get("price"))
                size = float(trade.get("size"))
                timestamp = parser.parse(trade.get("time")).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, size, timestamp])
            return l
        except:
            timestamp = parser.parse(response.get("data").get("timestamp")).strftime('%Y-%m-%d %H:%M:%S')
            return None


    ## OKX ###

    def okx_option_oi_lookup(self, response : dict, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
        """
            side : P, C
            returns : strikes, countdowns, ois, price, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            strikes = np.array([float(x["instId"].split('-')[-2]) for x in response["data"]["data"] if x["instId"].split('-')[-1] == side])
            countdowns = np.array([calculate_option_time_to_expire_okex(x["instId"].split('-')[2]) for x in response["data"]["data"] if x["instId"].split('-')[-1] == side])
            ois = np.array([float(x["oiCcy"]) for x in response["data"]["data"] if x["instId"].split('-')[-1] == side])
            timestamp = int(response.get("data").get("data")[0].get("ts")) / 1000
            timestamp = arrow.get(timestamp).format('YYYY-MM-DD HH:mm:ss')
            return strikes, countdowns, ois, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None






    def okx_GTA_lookup(self, response : dict) -> Tuple[float, float, str]:
        """
            ratio, pricem, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            ratio = float(response.get("data").get("data")[0][1])
            timestamp = int(response.get("data").get("data")[0][0]) / 1000
            timestamp = arrow.get(timestamp).format('YYYY-MM-DD HH:mm:ss')
            return ratio, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None




    def okx_depth_lookup(self, response : dict, side : str) -> Tuple[list, str]:
        """
            side : asks, bids
            returns: returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        try:
            books = response.get("data").get("data")[0][side]
            formated_books = [[float(x[0]), float(x[1])] for x in books]
            timestamp = arrow.get(int(response.get("data").get("data")[0]["ts"]) / 1000).format('YYYY-MM-DD HH:mm:ss')
            if instrument == "btcusd" and insType == "perpetual":
                formated_books = [ [x[0], self.unit_conversion_dict.get("okx_perp_btcusd")(x[1], price)]  for x in formated_books]
            return formated_books, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None




    def okx_trades_lookup(self, response : dict) -> list:
        """
            [[side, price, amount, timestamp]]
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        try:
            l = []
            for trade in response.get("data").get("data"):
                side = trade.get("side")
                price = float(trade.get("px"))
                amount = float(trade.get("sz"))
                timestamp = arrow.get(int(trade["ts"]) / 1000).format('YYYY-MM-DD HH:mm:ss')
                if instrument in ["btcusd"] and insType == "perpetual":
                    self.unit_conversion_dict.get("okx_perp_btcusd")(amount, price)
                l.append([side, price, amount, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None




    def okx_funding_lookup(self, response : dict) -> Tuple[float, float, str]:
        """
            rate, price, timestamp
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            rate = float(response.get("data").get("data")[0].get("fundingRate"))
            timestamp = datetime.datetime.fromtimestamp(float(response.get("data").get("data")[0].get("ts")) / 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return rate, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def okx_liquidations_lookup(self, response : json) -> list:
        """
            [[side, price, amount, timestamp], ....]
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            l = []
            for liquidation in response.get("data").get("data"):
                ticker = liquidation.get("instFamily")
                if "BTC" in ticker:
                    for detail in liquidation.get("details"):
                        side = detail.get("side")
                        price = float(detail.get("bkPx"))
                        amount = float(detail.get("sz"))
                        if ticker in ["BTC-USD-SWAP", "BTC-USDT-SWAP"]:
                            if ticker == "BTC-USD-SWAP":
                                amount = self.unit_conversion_dict.get("okx_perp_btcusd")(amount, price)
                            if ticker == "BTC-USDT-SWAP":
                                pass
                        else:
                            return None
                        timestamp = detail.get("ts")
                        timestamp = datetime.datetime.fromtimestamp(float(timestamp) / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                        l.append([side, price, amount, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None

    def okx_OI_lookup(self, response : json) -> Tuple[float, float, str]:
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        try:
            oi = float(response.get("data").get("data")[0].get("oiCcy"))
            timestamp = float(response.get("data").get("data")[0].get("ts"))
            timestamp = datetime.datetime.fromtimestamp(float(timestamp) / 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return oi, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


## DERIBIT ###


    def deribit_option_oi_lookup(self, response : dict, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
        """
            side : P, C
        """
        response = json.loads(response)
        price = response["btc_price"]
        try:
            strikes = np.array([float(x["instrument_name"].split('-')[-2]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
            countdowns = np.array([calculate_option_time_to_expire_deribit(x["instrument_name"].split('-')[1]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
            ois = np.array([float(x["open_interest"]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
            timestamp = response.get("data").get("usOut")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**6).strftime('%Y-%m-%d %H:%M:%S')
            return strikes, countdowns, ois, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None



    # CRYPTO PANIC #

    def lookup_news(self, data):
        data = json.loads(data)
        try:
            results = data.get("data").get("results")
            timestamp = results[0].get("published_at")
            news = results[0].get("title")
            input_datetime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
            formatted_datetime = input_datetime.strftime('%Y-%m-%d %H:%M:%S')
            return news, formatted_datetime
        except:
            return None
        

    # Bingx
        
    def bingx_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        side = "bid" if side == "bids" else "ask"
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        try:
            if insType == "spot":
                if side == "ask":
                    for key in ["a", "asks"]:
                        books = response["data"].get("data").get(key, np.nan)
                        if books is np.nan:
                            continue
                        else:
                            break

                if side == "bid":
                    for key in ["b", "bids"]:
                        books = response["data"].get("data").get(key, np.nan)
                        if books is np.nan:
                            continue
                        else:
                            break

                if response["data"].get("E", np.nan) != np.nan:
                    try:
                        timestamp = datetime.datetime.fromtimestamp(float(response["data"].get("E")/ 10**3)).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                
                books = [[float(x[0]), float(x[1])] for x in books]

            if insType == "perpetual" and instrument == "btcusdt":
                books = response["data"].get("data").get("".join([side, "Coin"]))
                books = [[float(x[0]), float(x[1])] for x in books]
                timestamp = datetime.datetime.fromtimestamp(float(response["data"].get("T")/ 10**3)).strftime('%Y-%m-%d %H:%M:%S')
            return books, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    

    def bingx_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("data").get("lastFundingRate"))
            timestamp = response.get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            return funding, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    
    def bingx_OI_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            oi, price, timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        price = response.get("btc_price")
        try:
            openInterest = float(response.get("data").get("data").get("openInterest"))
            timestamp = response.get("data").get("data").get("time")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            openInterest = self.unit_conversion_dict.get("bingx_perp_btcusdt_OI")(openInterest, price)
            return openInterest, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    


    def bingx_trades_lookup(self, response : json) -> list:
        """
            [side, price, amount, timestamp]
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response.get("btc_price")
        if insType == "spot":
            try:
                l = []
                t = response.get("data").get("data")
                quantity = float(t.get("q"))
                price = float(t.get("p"))
                side = "sell" if t.get("m") is True else "buy"
                timestamp = float(t.get("T"))
                timestamp = datetime.datetime.fromtimestamp(timestamp / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
                return l
            except:
                return None
        if insType == "perpetual":
            try:
                l = []
                for t in response.get("data").get("data"):
                    quantity = float(t.get("q"))
                    price = float(t.get("p"))
                    side = "sell" if t.get("m") is True else "buy"
                    timestamp = float(t.get("T"))
                    timestamp = datetime.datetime.fromtimestamp(timestamp / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, quantity, timestamp])
                return l
            except:
                return None

    ### BITGET ###
        
    def bitget_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        if insType == 'perpetual':
            try:
                if side == "asks":
                    books = response.get("data").get("data")[0].get("asks")
                if side == "bids":
                    books = response.get("data").get("data")[0].get("bids")
                books = [list(map(lambda xx : float(xx), x)) for x in books]
                timestamp = datetime.datetime.fromtimestamp(float(response.get("data").get("data")[0].get("ts")) / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("response").get("data").get("asks")
                    if side == "bids":
                        books = response.get("data").get("response").get("data").get("bids")
                    books = [list(map(lambda xx : float(xx), x)) for x in books]
                    timestamp = datetime.datetime.fromtimestamp(response.get("data").get("response").get("requestTime") / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except:
                    return None
        if insType == 'spot':
            try:
                if side == "asks":
                    books = response.get("data").get("data")[0].get("asks")
                if side == "bids":
                    books = response.get("data").get("data")[0].get("bids")
                books = [list(map(lambda xx : float(xx), x)) for x in books]
                timestamp = datetime.datetime.fromtimestamp(float(response.get("data").get("data")[0].get("ts")) / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("data").get("asks")
                    if side == "bids":
                        books = response.get("data").get("data").get("bids")
                    books = [list(map(lambda xx : float(xx), x)) for x in books]
                    timestamp = datetime.datetime.fromtimestamp(response.get("data").get("requestTime") / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except:
                    return None

    def bitget_trades_lookup(self, response : json) -> list:
        """
            [side, price, amount, timestamp]
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response.get("btc_price")
        try:
            l = []
            for t in response.get("data").get("data"):
                quantity = float(t.get("size"))
                price = float(t.get("price"))
                side = str(t.get("side"))
                timestamp = float(t.get("ts"))
                timestamp = datetime.datetime.fromtimestamp(timestamp / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
            return l
        except:
            return None
    
    def bitget_OI_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("data")[0].get("fundingRate"))
            price = float(response.get("data").get("data")[0].get("lastPr"))
            OI = float(response.get("data").get("data")[0].get("holdingAmount"))
            timestamp = response.get("data").get("ts")
            timestamp = datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
            return funding, OI, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    
    ### DERIBIT ###

    def deribit_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        try:
            if side == "asks":
                books = response.get("data").get("params").get("data").get("asks")
                books = [[x[1], self.unit_conversion_dict.get("deribit_perp_btcusd")(x[1], price)] for x in books]
            if side == "bids":
                books = response.get("data").get("params").get("data").get("bids")
                books = [[x[1], self.unit_conversion_dict.get("deribit_perp_btcusd")(x[1], price)] for x in books]
            timestamp = response.get("data").get("params").get("data").get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
            return books, timestamp
        except:
            return None


    def deribit_OI_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = response.get("data").get("params").get("data").get("funding_8h")
            OI = response.get("data").get("params").get("data").get("open_interest")
            OI = self.unit_conversion_dict.get("deribit_perp_btcusd_OI")(OI, price)
            price = response.get("data").get("params").get("data").get("last_price")
            timestamp = response.get("data").get("params").get("data").get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
            return funding, OI, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    
    def deribit_trades_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            l = []
            for t in response.get("data").get("params").get("data"):
                side = t.get("direction")
                price = t.get("price")
                quantity = t.get("amount")
                quantity = self.unit_conversion_dict.get("deribit_perp_btcusd")(float(quantity), price)
                timestamp = t.get("timestamp")
                timestamp = datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    ### gateio ### 
        
    def gateio_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response["btc_price"]
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        if insType == "spot":
            try:
                if side == "asks":
                    books = response.get("data").get("result").get("a")
                    books = [[float(x[0]), float(x[1])] for x in books]
                if side == "bids":
                    books = response.get("data").get("result").get("b")
                    books = [[float(x[0]), float(x[1])] for x in books]
                timestamp = response.get("data").get("result").get("t")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("asks")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    if side == "bids":
                        books = response.get("data").get("bids")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    timestamp = response.get("data").get("update")
                    timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except Exception as e:
                    # print(f"An error occurred: {e}")
                    return None

        if insType == "perpetual":
            try:
                if side == "asks":
                    books = response.get("data").get("asks")
                    books = [[float(x.get("p")), self.unit_conversion_dict.get("gateio_perp_btcusdt")(float(x.get("s")), price)] for x in books]
                if side == "bids":
                    books = response.get("data").get("bids")
                    books = [[float(x.get("p")), self.unit_conversion_dict.get("gateio_perp_btcusdt")(float(x.get("s")), price)] for x in books]
                timestamp = response.get("data").get("update")
                timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                return None
        
    def gateio_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("funding_rate"))
            timestamp = float(response.get("timestamp"))
            timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            return funding, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
        

    def gateio_trades_lookup(self, response : json) -> list:
        """
            [side, price, amount, timestamp]
        """
        response = json.loads(response)
        instrument = response["instrument"]
        insType = response["insType"]
        price = response.get("btc_price")
        if insType == "spot":
            try:
                l = []
                t = response.get("data").get("result")
                quantity = float(t.get("amount"))
                price = float(t.get("price"))
                side = t.get("side")
                timestamp = float(response.get("data").get("time_ms"))
                timestamp = datetime.datetime.fromtimestamp(timestamp / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
                return l
            except:
                return None
        if insType == "perpetual":
            try:
                l = []
                for t in response.get("data"):
                    quantity = float(t.get("size")) 
                    quantity = self.unit_conversion_dict.get("gateio_perp_btcusdt")(quantity, price)
                    price = float(t.get("price"))
                    side = "sell" if float(t.get("size")) < 0 else "buy"
                    timestamp = float(t.get("create_time_ms"))
                    timestamp = datetime.datetime.fromtimestamp(timestamp / 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, quantity, timestamp])
                return l
            except:
                return None


    def gateio_OI_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            OI, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            OI = float(response.get("data")[0].get("open_interest_usd")) 
            OI = self.unit_conversion_dict.get("gateio_perp_btcusdt")(OI, price)
            timestamp = float(response.get("data")[0].get("time"))
            timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            return OI, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    

    def gateio_liquidations_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            l = []
            liquidations = response.get("data")
            for liquidation in liquidations:
                if liquidation.get("contract") == "BTC_USDT":
                    side = "buy" if float(liquidation.get("size")) > 0 else "sell"
                    price = float(liquidation.get("fill_price"))
                    amount = float(liquidation.get("size"))
                    amount = self.unit_conversion_dict.get("gateio_perp_btcusdt")(amount, price)
                    timestamp = liquidation.get("time")
                    timestamp = datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, amount, timestamp])
            return l
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    
    ## HTX ###
        
    def htx_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "perpetual":
            try:
                if side == "asks":
                    books = response.get("data").get("tick").get("asks")
                    books = [[float(x[0]), self.unit_conversion_dict.get("htx_perp_btcusdt")(float(x[1]), price)] for x in books]
                if side == "bids":
                    books = response.get("data").get("tick").get("bids")
                    books = [[float(x[0]), self.unit_conversion_dict.get("htx_perp_btcusdt")(float(x[1]), price)] for x in books]
                timestamp = response.get("data").get("ts")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        if insType == "spot":
            try:
                if side == "asks":
                    books = response.get("data").get("tick").get("asks")
                    books = [[float(x[0]), float(x[1])] for x in books]
                if side == "bids":
                    books = response.get("data").get("tick").get("bids")
                    books = [[float(x[0]), float(x[1])] for x in books]
                timestamp = response.get("data").get("ts")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        
    def htx_trades_lookup(self, response : json) -> Tuple[float, float, str]:
        """

            returns funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "perpetual":
            try:
                l = []
                for trade in response.get("data").get("tick").get("data"):
                    side = trade.get("direction")
                    price = float(trade.get("price"))
                    quantity = abs(trade.get("quantity"))
                    quantity = self.unit_conversion_dict.get("htx_perp_btcusdt")(quantity, price)
                    timestamp = trade.get("ts")
                    timestamp = datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        if insType == "spot":
            try:
                l = []
                for trade in response.get("data").get("tick").get("data"):
                    side = trade.get("direction")
                    price = float(trade.get("price"))
                    quantity = abs(trade.get("amount"))
                    timestamp = trade.get("ts")
                    timestamp = datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        

    def htx_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            OI, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("data")[0].get("close"))
            timestamp = float(response.get("data").get("ts"))
            timestamp = datetime.datetime.fromtimestamp(timestamp / 10 **3).strftime('%Y-%m-%d %H:%M:%S')
            return funding, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    def htx_OI_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            OI, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            OI = float(response.get("data").get("data").get("tick")[0].get("value"))
            OI = self.unit_conversion_dict.get("htx_perp_btcusdt_OI")(OI, price)
            timestamp = float(response.get("data").get("ts"))
            timestamp = datetime.datetime.fromtimestamp(timestamp / 10 **3).strftime('%Y-%m-%d %H:%M:%S')
            return OI, price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None


    ### Kucoin ###
    
    def kucoin_OI_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("data").get("fundingFeeRate"))
            price = float(response.get("data").get("data").get("lastTradePrice"))
            OI = float(response.get("data").get("data").get("openInterest"))
            timestamp = response.get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            return funding, self.unit_conversion_dict.get("kucoin_perp_btcusdt")(OI) , price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    

    def kucoin_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "spot":
            try:
                if side == "asks":
                    books = response.get("data").get("data").get("changes").get("asks")
                    books = [[float(x[0]), float(x[1])] for x in books]
                if side == "bids":
                    books = response.get("data").get("data").get("changes").get("bids")
                    books = [[float(x[0]), float(x[1])] for x in books]
                timestamp = response.get("data").get("data").get("time")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                books = [[price, 0]] if len(books) == 0 else books 
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("data").get("asks")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    if side == "bids":
                        books = response.get("data").get("data").get("bids")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    timestamp = response.get("data").get("data").get("time")
                    timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except:
                    return None
        if insType == "perpetual":
                try:
                    books = response.get("data").get("data").get("change")
                    books = books.split(",")
                    s = books.pop(1)
                    s = "asks" if s == "sell" else "bids"
                    books = list(map(lambda x: float(x), books))
                    books[1] = self.unit_conversion_dict.get("kucoin_perp_btcusdt")(books[1])
                    timestamp = response.get("data").get("data").get("timestamp")
                    timestamp =  datetime.datetime.fromtimestamp(timestamp / 10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    books = [[price, 0]] if len(books) == 0 else [books]
                    return books, timestamp
                except:
                    try:
                        if side == "asks":
                            books = response.get("data").get("data").get("asks")
                            books = [[x[0], self.unit_conversion_dict.get("kucoin_perp_btcusdt")(x[1])] for x in books]
                        if side == "bids":
                            books = response.get("data").get("data").get("bids")
                            books = [[x[0], self.unit_conversion_dict.get("kucoin_perp_btcusdt")(x[1])] for x in books]
                        timestamp = response.get("data").get("data").get("ts")
                        timestamp =  datetime.datetime.fromtimestamp(timestamp / 10 ** 9).strftime('%Y-%m-%d %H:%M:%S')
                        return books, timestamp
                    except:
                        return None
            

    def kucoin_trades_lookup(self, response : json) -> Tuple[float, float, str]:
        """

            returns funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "perpetual":
            try:
                l = []
                trade = response.get("data").get("data")
                side = trade.get("side")
                price = float(trade.get("price"))
                quantity = float(trade.get("size"))
                quantity = self.unit_conversion_dict.get("kucoin_perp_btcusdt")(quantity)
                timestamp = response.get("timestamp")
                timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        if insType == "spot":
            try:
                l = []
                trade = response.get("data").get("data")
                side = trade.get("side")
                price = float(trade.get("price"))
                quantity = float(trade.get("size"))
                timestamp = response.get("timestamp")
                timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
            


### MEXC ###
        
    def mexc_OI_funding_lookup(self, response : json) -> Tuple[float, float, str]:
        """
            funding, OI, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        try:
            funding = float(response.get("data").get("data").get("riseFallRate"))
            price = float(response.get("data").get("data").get("lastPrice"))
            OI = float(response.get("data").get("data").get("holdVol"))
            timestamp = response.get("timestamp")
            timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            return funding, self.unit_conversion_dict.get("mexc_perp_btcusdt")(OI) , price, timestamp
        except:
            timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            return None
    


    def mexc_depth_lookup(self, response : json, side : str) -> Tuple[list, str]:
        """
            side : bids, asks
            Must return a list of float and timestamp in string format
            [
                [float(price), float(amount)],
                .....
            ]
            returns: [[price, amount]...], timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "spot":
            try:
                if side == "asks" and "asks" in response.get("data").get("d"):
                    books = response.get("data").get("d").get("asks")
                    books = [[float(x.get("p")), float(x.get("v"))] for x in books]
                if side == "bids" and "bids" in response.get("data").get("d"):
                    books = response.get("data").get("d").get("bids")
                    books = [[float(x.get("p")), float(x.get("v"))] for x in books]
                timestamp = response.get("data").get("t")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("response").get("asks")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    if side == "bids":
                        books = response.get("data").get("response").get("bids")
                        books = [[float(x[0]), float(x[1])] for x in books]
                    timestamp = response.get("data").get("response").get("timestamp")
                    timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except:
                    return None
        if insType == "perpetual":
            try:
                if side == "asks":
                    books = response.get("data").get("data").get("asks")
                    books = [[float(x[0]), self.unit_conversion_dict.get("mexc_perp_btcusdt")(float(x[1]))] for x in books]
                if side == "bids":
                    books = response.get("data").get("data").get("bids")
                    books = [[float(x[0]), self.unit_conversion_dict.get("mexc_perp_btcusdt")(float(x[1]))] for x in books]
                timestamp = response.get("data").get("ts")
                timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                return books, timestamp
            except:
                try:
                    if side == "asks":
                        books = response.get("data").get("response").get("data").get("asks")
                        books = [[float(x[0]), float(x[1] * 0.0001)] for x in books]
                    if side == "bids":
                        books = response.get("data").get("response").get("data").get("bids")
                        books = [[float(x[0]), float(x[1] * 0.0001)] for x in books]
                    timestamp = response.get("data").get("response").get("data").get("timestamp")
                    timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
                    return books, timestamp
                except:
                    return None


    def mexc_trades_lookup(self, response : json) -> Tuple[float, float, str]:
        """

            returns funding, price, timestamp
        """
        response = json.loads(response)
        price = float(response.get("btc_price"))
        insType = response["insType"]
        if insType == "spot":
            try:
                l = []
                for trade in  response.get("data").get("d").get("deals"):
                    side = "buy" if trade.get("S") == 1 else "sell"
                    price = float(trade.get("p"))
                    quantity = float(trade.get("v")) 
                    timestamp = response.get("data").get("t")
                    timestamp = datetime.datetime.fromtimestamp(timestamp /10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                    l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None
        if insType == "perpetual":
            try:
                l = []
                trade = response.get("data").get("data")
                side = "buy" if trade.get("S") == 1 else "sell"
                price = float(trade.get("p"))
                quantity = float(trade.get("v"))
                quantity = self.unit_conversion_dict.get("mexc_perp_btcusdt")(quantity)
                timestamp = response.get("data").get("ts")
                timestamp = datetime.datetime.fromtimestamp(timestamp /10 ** 3).strftime('%Y-%m-%d %H:%M:%S')
                l.append([side, price, quantity, timestamp])
                return l
            except:
                timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                return None