import requests

url_usd = "https://dapi.binance.com/dapi/v1/openInterest?symbol=BTCUSD_PERP"

url_usdt = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"

print(requests.get(url_usd).json)