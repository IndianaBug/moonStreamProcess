import sys
import time
import json
from os.path import abspath, dirname
from IPython.display import display, clear_output

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import flow, synthesis, frametest
from lookups import btc as btc_lookups
from lookups import unit_conversion_btc

lookups_btc = btc_lookups(unit_conversion_btc)

class btc(frametest.testerOption):
    """
        Merger of bybit, deribit and okx options
    """

    def __init__(self, pranges, expiry_windows, exchanges = ["bybit", "okx", "deribit"]):
        self.pranges = pranges
        self.exchanges = exchanges
        self.expiry_windows = expiry_windows
        self.axis = {
            "deribit" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups_btc.deribit_option_oi_lookup),
            "bybit" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups_btc.bybit_option_oi_lookup),
            "okx" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups_btc.okx_option_oi_lookup),
                    }
        for exchange in self.axis.keys():
            if exchange not in self.exchanges:
                del self.axis[exchange]
        self.aggregator = synthesis.OOImerger("btcusdt", "options", self.expiry_windows, self.pranges, self.axis)
        self.data = {}

    def merge(self):
        self.aggregator.mergeoi()
        self.data = self.aggregator.data

    def add_deribit(self, data):
        self.aggregator.axis["deribit"].input_oi(data)

    def add_bybit(self, data):
        self.aggregator.axis["bybit"].input_oi(data)

    def add_okx(self, data):
        self.aggregator.axis["okx"].input_oi(data)

    def merge(self):
        self.aggregator.mergeoi()

    def retrive_data(self, side=None, key_1=None, key_2=None):
        self.aggregator(side, key_1, key_2)

    # Testing

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "deribit_btcusd_option_OI", "bybit_btcusdt_option_OI", "okx_btc_option_OI", 
            ]

        functions_list = [
            self.add_deribit, self.add_bybit, self.add_okx
        ]
        v = {
            x : { 
            "data" : json.load(open("".join(["data/", x, ".json"]))),
            "lookup" : f,
            } 
            for x, f in zip(files, functions_list)
            }

        for key in v.keys():
            for d in v[key]["data"]:
                d = json.dumps(d)
                v[key]["lookup"](d)