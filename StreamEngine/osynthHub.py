import sys
import time
import json
from os.path import abspath, dirname
from IPython.display import display, clear_output

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import lookups
import flow
import synthesis
from frametest import testerOption


class btcSynth(testerOption):
    """
        Merger of bybit, deribit and okx options
    """

    def __init__(self, pranges, expiry_windows):
        self.pranges = pranges
        self.expiry_windows = expiry_windows
        self.aggregator = synthesis.OOImerger("btcusdt", "options", self.expiry_windows, self.pranges, {
                                                    "deribit" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups.deribit_option_oi_lookup),
                                                    "bybit" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups.bybit_option_oi_lookup),
                                                    "okx" : flow.oiflowOption("deribit", "btc", "options", self.pranges,  self.expiry_windows, lookups.okx_option_oi_lookup),
                                                                                                        }
                                            )

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