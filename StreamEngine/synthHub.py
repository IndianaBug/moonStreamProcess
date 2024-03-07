import sys
import time
import json
from os.path import abspath, dirname

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import synthesis
import StreamEngine.coins.btcflow as btcflow


class btcSynth():

    def __init__(self, level_size, exchanges = ["binance", "okx", "bybit", "bitget", "bingx", "mexc", "kucoin", "deribit", "coinbase", "htx", "gateio"]):
        self.exchanges = exchanges
        self.level_size = level_size
        self.axis = {
            "binance" : btcflow.binance_flow(level_size),
            "okx" : btcflow.okx_flow(level_size),
            "bybit" : btcflow.bybit_flow(level_size),
            "bingx" : btcflow.bingx_flow(level_size),
            "mexc" : btcflow.mexc_flow(level_size),
            "kucoin" : btcflow.kucoin_flow(level_size),
            "deribit" : btcflow.deribit_flow(level_size),
            "coinbase" : btcflow.coinbase_flow(level_size),
            "htx" : btcflow.htx_flow(level_size),
            "gateio" : btcflow.gateio_flow(level_size),
            "bitget" : btcflow.bitget_flow(level_size),
        }
        self.books = {
            "spot" : synthesis.booksmerger("btc", "spot", axis = {
                self.axis[exchange][books] for exchange in self.axis.keys() for books in self.axis[exchange].spot_axis.keys()
            }
                                           ),
            "perpetual" : synthesis.booksmerger("btc", "spot", axis = {
                self.axis[exchange][books] for exchange in self.axis.keys() for books in self.axis[exchange].perpetual_axis.keys()
            }
                                           )
        }