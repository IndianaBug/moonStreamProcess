import json
import numpy as np
import datetime
from dateutil import parser
import arrow
from typing import Tuple
from utilis import calculate_option_time_to_expire_deribit, calculate_option_time_to_expire_okex, calculate_option_time_to_expire_bybit

### BINANCE ###

def binance_liquidations_lookup(response : json) -> Tuple[str, float, float, str]:
    response = json.loads(response)
    price = float(response.get("btc_price"))
    try:
        amount = float(response.get("data").get("o").get("q"))
        side = response.get("data").get("o").get("S").lower()
        price = float(response.get("data").get("o").get("p"))
        timestamp = response.get("data").get("o").get("T")
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return side, price, amount, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp


def binance_funding_lookup(response : json) -> Tuple[float, float, str]:
    response = json.loads(response)
    price = float(response.get("btc_price"))
    try:
        funding = float(response.get("data")[0].get("fundingRate"))
        timestamp = response.get("data")[0].get("fundingTime")
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return funding, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, timestamp 



def binance_OI_lookup(response : json) -> Tuple[float, float, str]:
    response = json.loads(response)
    price = response.get("btc_price")
    try:
        openInterest = float(response.get("data").get("openInterest"))
        timestamp = response.get("data").get("time")
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return openInterest, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, timestamp




def binance_GTA_TTA_TTP_lookup(response : json) -> Tuple[float, float, float, str]:
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
            return np.nan, np.nan, np.nan, price, timestamp


def binance_trades_lookup(response : json) -> Tuple[str, float, float, str]:

    response = json.loads(response)
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
        return side, price, quantity, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp


def binance_depth_lookup(response : json, side : str) -> Tuple[list, str]:
    """
        side : bids, asks
        Must return a list of float and timestamp in string format
        [
            [float(price), float(amount)],
            .....
        ]
    """
    side = "bid" if side == "bids" else "ask"
    response = json.loads(response)
    print(response["timestamp"])
    timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')

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
    
    return books, timestamp



### Bybit ###

def bybit_GTA_lookup(response : json) -> Tuple[float, float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        buyRation = float(response.get("data").get("result").get("list")[0].get("buyRatio"))
        sellRation = float(response.get("data").get("result").get("list")[0].get("sellRatio"))
        timestamp = int(response.get("data").get("result").get("list")[0].get("timestamp"))
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return buyRation, sellRation, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp



def bybit_depth_lookup(response : json, side : str) -> Tuple[list, str]:
    """ 
        side : "bids" for books
               "asks" for asks
    """
    side = "a" if side == "asks" else "b"
    response = json.loads(response)
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
        books = [[float(x[0]), float(x[1])] for x in books]
        return books, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return [[0, 0]], timestamp


def bybit_OI_funding_lookup(response : json) -> Tuple[float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        funding = float(response.get("data").get("data").get("fundingRate"))
        timestamp = float(response.get("data").get("ts"))
        openInterestValue = float(response.get("data").get("data").get("openInterestValue"))
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return funding, openInterestValue, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp


def bybit_liquidations_lookup(response : json) -> Tuple[str, float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        side = response.get("data").get("data").get("side", np.nan).lower()
        price = float(response.get("data").get("data").get("price", np.nan))
        size = float(response.get("data").get("data").get("size", np.nan))
        timestamp = response.get("data").get("data").get("updatedTime", np.nan)
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return side, price, size, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp



def bybit_trade_lookup(response : json) -> Tuple[str, float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        side = response.get("data").get("data")[0].get("S", np.nan).lower()
        price = float(response.get("data").get("data")[0].get("p"))
        size = float(response.get("data").get("data")[0].get("v"))
        timestamp = response.get("data").get("data")[0].get("T")
        timestamp = datetime.datetime.fromtimestamp(timestamp/ 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return side, price, size, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, price, np.nan, timestamp


def bybit_option_oi_lookup(response : json, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
    """
        side : P, C
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
        return np.nan, np.nan, np.nan, price, timestamp




### COINBASE ###

def coinbase_depth_lookup(response, side) -> Tuple[list, str]:
    """
        side : asks, bids
    """
    side = "offer" if side == "asks" else "bid"

    response = json.loads(response)
    # The first response may be a subscription info
    if response.get("data").get("events", np.nan) != np.nan:
        try: 
            if response.get("data").get("events")[0].get("subscriptions", None) != None:  
                return [[0, 0]], parser.parse(response.get("data").get("timestamp")).strftime('%Y-%m-%d %H:%M:%S')
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



def coinbase_trades_lookup(response : dict) -> Tuple[str, float, float, str]:

    response = json.loads(response)

    if response.get("data").get("events") != np.nan: 
        if response.get("data").get("events")[0].get("subscriptions") != np.nan:
            return np.nan, np.nan, np.nan, parser.parse(response.get("data").get("timestamp")).strftime('%Y-%m-%d %H:%M:%S')

    if response.get("data").get("events") != np.nan:
        if response.get("data").get("events")[0].get("type") != np.nan:
          side = response.get("data").get("events")[0].get("trades")[0].get("side").lower()
          price = float(response.get("data").get("events")[0].get("trades")[0].get("price"))
          amount = float(response.get("data").get("events")[0].get("trades")[0].get("size"))
          timestamp = parser.parse(response.get("data").get("events")[0].get("trades")[0].get("time")).strftime('%Y-%m-%d %H:%M:%S')
          return side, price, amount, timestamp


## OKX ###

def okx_option_oi_lookup(response : dict, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
    """
        side : P, C
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
        return np.nan, np.nan, np.nan, price, timestamp






def okx_GTA_lookup(response : dict) -> Tuple[float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        ratio = float(response.get("data").get("data")[0][1])
        timestamp = int(response.get("data").get("data")[0][0]) / 1000
        timestamp = arrow.get(timestamp).format('YYYY-MM-DD HH:mm:ss')
        return ratio, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(response["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, timestamp




def okx_depth_lookup(response : dict, side : str) -> Tuple[list, str]:
    """
        side : asks, bids
    """
    response = json.loads(response)
    price = response["btc_price"]
    try:
        books = response.get("data").get("data")[0][side]
        formated_books = [[float(x[0]), float(x[1])] for x in books]
        timestamp = arrow.get(int(response.get("data").get("data")[0]["ts"]) / 1000).format('YYYY-MM-DD HH:mm:ss')
        return formated_books, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return [[0, 0]], timestamp




def okx_trades_lookup(response : dict) -> Tuple[str, float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:    
        side = response.get("data").get("data")[0].get("side")
        price = float(response.get("data").get("data")[0].get("px"))
        amount = float(response.get("data").get("data")[0].get("sz"))
        timestamp = arrow.get(int(response.get("data").get("data")[0]["ts"]) / 1000).format('YYYY-MM-DD HH:mm:ss')
        return side, price, amount, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp





def okx_funding_lookup(response : dict) -> Tuple[float, float, str]:
    response = json.loads(response)
    price = response["btc_price"]
    try:
        rate = float(response.get("data")[0].get("fundingRate"))
        timestamp = datetime.fromtimestamp(float(response.get("data")[0].get("ts")) / 10**3).strftime('%Y-%m-%d %H:%M:%S')
        return rate, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, timestamp


def okex_liquidations_lookup(response : json) -> Tuple[str, float, float, str]:
    response = json.load(response)
    price = response["btc_price"]
    try:
        ticker = response.get("data").get("data")[0].get("instFamily")
        timestamp = datetime.fromtimestamp(float(response.get("data").get("data")[0].get("details")[0].get("ts")) / 10**3).strftime('%Y-%m-%d %H:%M:%S')
        if "BTC" in ticker:
            side = response.get("data").get("data")[0].get("details")[0].get("side")
            price = float(response.get("data").get("data")[0].get("details")[0].get("bkPx"))
            amount = float(response.get("data").get("data")[0].get("details")[0].get("sz"))
            return side, price, amount, timestamp
        else:
            return 0, 0, 0, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, timestamp



## DERIBIT ###


def deribit_option_oi_lookup(response : dict, side : str) -> Tuple[np.array, np.array, np.array, float, str]:
    """
        side : P, C
    """
    response = json.loads(response)
    price = response["btc_price"]
    try:
        strikes = np.array([float(x["instrument_name"].split('-')[-2]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
        countdowns = np.array([calculate_option_time_to_expire_deribit(x["instrument_name"].split('-')[1]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
        ois = np.array([float(x["open_interest"]) for x in response["data"]["result"] if x["instrument_name"].split('-')[-1] == side])
        timestamp = parser.parse(response.get("data").get("usOut")).strftime('%Y-%m-%d %H:%M:%S')
        return strikes, countdowns, ois, price, timestamp
    except:
        timestamp = datetime.datetime.fromtimestamp(r["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
        return np.nan, np.nan, np.nan, np.nan, timestamp
