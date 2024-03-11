import sys
import json
from os.path import abspath, dirname
import pandas as pd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from copy import copy
import warnings
warnings.filterwarnings("ignore")

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngine")

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import synthesis
import spotperp.btc as btcflow
import option.btc as btcoflow



class btcSynth():

    def __init__(self, level_size : int, pranges : np.array, expiry_windows : np.array, 
                 exchanges_spot_perp : list = ["binance", "okx", "bybit", "bitget", "bingx", "kucoin", "deribit", "coinbase", "htx", "gateio"],
                 exchanges_option : list = ["bybit", "okx", "deribit"],
                 book_ceil_thresh : int = 5
                 ):
        """
            level size : 
                Incrimented tier at which trading information, encompassing order books, trades, liquidations, adjustments and open interest occurances are aggregated
                Used for spot and perpetual markets 
                Ex : 20
            pranges : 
                The level size as percerage from the present market price for aggregating options open interest by strike price by expiry window
                Used for options
                Ex : np.array([0.0, 1.0, 2.0, 5.0, 10.0]) will create percentage ranges from 0:1, 1:2 ... -1:0, -2:-1 ...
            expiry_windows : 
                The time horizon size as percerage from the present market price for aggregating options open interest by strike price
                Used for options
                Ex : np.array([0.0, 1.0, 3.0, 7.0]) will create timehorizon ranges from 0:1, 1:3 ... -1:0, -3:-1 ...
            exchanges_spot_perp:
                Selects available exchanges for spot and perpetual instruments
                available : ["binance", "okx", "bybit", "bitget", "bingx", "mexc", "kucoin", "deribit", "coinbase", "htx", "gateio"]
            exchanges_spot_perp:
                Selects available exchanges options
                available : ["bybit", "okx", "deribit"]
            book_ceil_thresh : exclude storing order book data for entries that deviate by a significant percentage from the current market price
            
        """
        self.exchanges_spot_perp = exchanges_spot_perp
        self.exchanges_option = exchanges_option
        self.all_exchange = ["binance", "okx", "bybit", "bitget", "bingx", "mexc", "kucoin", "deribit", "coinbase", "htx", "gateio"]
        self.level_size = level_size
        self.pranges = pranges
        self.expiry_windows = expiry_windows
        self.axis = {
            "binance" : btcflow.binance_flow(level_size, book_ceil_thresh),
            "okx" : btcflow.okx_flow(level_size, book_ceil_thresh),
            "bybit" : btcflow.bybit_flow(level_size, book_ceil_thresh),
            "bingx" : btcflow.bingx_flow(level_size, book_ceil_thresh),
            "mexc" : btcflow.mexc_flow(level_size, book_ceil_thresh),
            "kucoin" : btcflow.kucoin_flow(level_size, book_ceil_thresh),
            "deribit" : btcflow.deribit_flow(level_size, book_ceil_thresh),
            "coinbase" : btcflow.coinbase_flow(level_size, book_ceil_thresh),
            "htx" : btcflow.htx_flow(level_size, book_ceil_thresh),
            "gateio" : btcflow.gateio_flow(level_size, book_ceil_thresh),
            "bitget" : btcflow.bitget_flow(level_size, book_ceil_thresh),
        }
        self.axis = {exchange: value for exchange, value in self.axis.items() if exchange in self.exchanges_spot_perp}

        self.books = {
            "spot" : synthesis.booksmerger("btc", "spot", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].spot_axis["books"][flow] for exchange in self.axis.keys() for flow in self.axis[exchange].spot_axis["books"].keys()
            }
                                           ),
            "perpetual" : synthesis.booksmerger("btc", "perpetul", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["books"][flow] for exchange in self.axis.keys() for flow in self.axis[exchange].perpetual_axis["books"].keys()
            }
                                           ),
        }
        self.trades = {
            "spot" : synthesis.tradesmerger("btc", "spot", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].spot_axis["trades"][flow] for exchange in self.axis.keys() if exchange != "deribit" for flow in self.axis[exchange].spot_axis["trades"].keys()
            }
                                           ),
            "perpetual" : synthesis.tradesmerger("btc", "perpetul", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["trades"][flow] for exchange in self.axis.keys() if exchange != "coinbase" for flow in self.axis[exchange].perpetual_axis["trades"].keys() 
            }
                                           ),
        }

        self.adjustmests = {
            "spot" : synthesis.booksadjustments("btc", "spot", self.books["spot"], self.trades["spot"]),
            "perpetual" : synthesis.booksadjustments("btc", "perpetual", self.books["perpetual"], self.trades["perpetual"])
        }

        self.oifunding =  synthesis.oiomnifier("btc", "perpetul", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["oifunding"][flow] for exchange in self.axis.keys() if exchange != "coinbase" for flow in self.axis[exchange].perpetual_axis["oifunding"].keys() 
            }
                                           )

        self.liquidations =  synthesis.lomnifier("btc", "perpetul", axis = {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["liquidations"][flow] for exchange in self.axis.keys() if exchange in ["binance", "okx", "bybit", "gateio"] for flow in self.axis[exchange].perpetual_axis["liquidations"].keys() 
            }
                                           )

        self.positionsTTA =  synthesis.indomnifier("btc", "perpetul", "TTA",  {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["indicators"][flow] for exchange in self.axis.keys() if exchange in ["binance"] for flow in self.axis[exchange].perpetual_axis["indicators"].keys() if "TTA" in flow 
            },
            self.oifunding
                                           )

        self.positionsTTP =  synthesis.indomnifier("btc", "perpetul", "TTP", {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["indicators"][flow] for exchange in self.axis.keys() if exchange in ["binance"] for flow in self.axis[exchange].perpetual_axis["indicators"].keys() if "TTP" in flow
            },
            self.oifunding
                                           )

        self.positionsGTA =  synthesis.indomnifier("btc", "perpetul", "GTA", {
                "_".join([exchange, flow]) : self.axis[exchange].perpetual_axis["indicators"][flow] for exchange in self.axis.keys() if exchange in ["binance", "okx", "bybit"] for flow in self.axis[exchange].perpetual_axis["indicators"].keys() if "GTA" in flow
            },
            self.oifunding
                                           )

        # options
        self.optionoi = btcoflow.btc(pranges, expiry_windows, exchanges_option)
        self.optionoi.axis = {exchange: value for exchange, value in self.optionoi.axis.items() if exchange in self.exchanges_option}
        

        self.data = dict()


    def merge(self):
        for insType in self.books.keys():
            self.books[insType].merge_snapshots()
            self.data["_".join(["books", insType])] = self.books[insType].snapshotO

        for insType in self.trades.keys():
            self.trades[insType].merge_snapshots()
            self.data["_".join(["trades", insType])] = self.trades[insType].snapshotO

        for insType in self.adjustmests.keys():
            self.adjustmests[insType].get_adjusted_orders()
            self.data["_".join(["adjustmests", insType])] = self.adjustmests[insType].data

        self.oifunding.merge_snapshots()
        self.data["_".join(["oifunding", "perpetual"])] = self.oifunding.snapshot

        self.liquidations.merge_snapshots()
        self.data["_".join(["liquidations", "perpetual"])] = self.liquidations.snapshot

        self.positionsTTA.merge_ratios()
        self.data["_".join(["TTA", "perpetual"])] = self.positionsTTA.data

        self.positionsTTP.merge_ratios()
        self.data["_".join(["TTP", "perpetual"])] = self.positionsTTP.data

        self.positionsGTA.merge_ratios()
        self.data["_".join(["GTA", "perpetual"])] = self.positionsGTA.data

        self.optionoi.aggregator.mergeoi()
        self.data["_".join(["oi", "option"])] = self.optionoi.aggregator.data

        self.data = self.flatten_data(self.data)
        
    def ratrive_data(self, data=None, type_=None):
        """
            'spot_books', 'perp_books', 'timestamp', 'spot_buyVol', 'spot_sellVol', 'spot_open', 'spot_close', 'spot_low', 'spot_high', 'spot_Vola', 
            'spot_VolProfile', 'spot_buyVolProfile', 'spot_sellVolProfile', 'spot_numberBuyTrades', 'spot_numberSellTrades', 'spot_orderedBuyTrades', 
            'spot_orderedSellTrades', 'perp_buyVol', 'perp_sellVol', 'perp_open', 'perp_close', 'perp_low', 'perp_high', 'perp_Vola', 'perp_VolProfile', 
            'perp_buyVolProfile', 'perp_sellVolProfile', 'perp_numberBuyTrades', 'perp_numberSellTrades', 'perp_orderedBuyTrades', 'perp_orderedSellTrades', 
            'spot_voids', 'spot_reinforces', 'spot_totalVoids', 'spot_totalReinforces', 'spot_totalVoidsVola', 'spot_voidsDuration', 'spot_voidsDurationVola', 
            'spot_reinforcesDuration', 'spot_reinforcesDurationVola', 'perp_voids', 'perp_reinforces', 'perp_totalVoids', 'perp_totalReinforces', 'perp_totalVoidsVola', 
            'perp_voidsDuration', 'perp_voidsDurationVola', 'perp_reinforcesDuration', 'perp_reinforcesDurationVola', 'perp_weighted_funding', 'perp_total_oi', 
            'perp_oi_increases', 'perp_oi_increases_Vola', 'perp_oi_decreases', 'perp_oi_decreases_Vola', 'perp_oi_turnover', 'perp_oi_turnover_Vola', 'perp_oi_total',
            'perp_oi_total_Vola', 'perp_oi_change', 'perp_oi_Vola', 'perp_orderedOIChanges', 'perp_OIs_per_instrument', 'perp_fundings_per_instrument', 
            'liquidations_perp_longsTotal', 'liquidations_perp_longs', 'liquidations_perp_shortsTotal', 'liquidations_perp_shorts', 'TTA_perp_ratio', 'TTP_perp_ratio', 
            'GTA_perp_ratio', {'oi_option_puts_0', 'oi_option_puts_10', 'oi_option_calls_0', 'oi_option_calls_7_10'} -- depends on pranges, 'perp_liquidations_longsTotal', 'perp_liquidations_longs',
            'perp_liquidations_shortsTotal', 'perp_liquidations_shorts', 'perp_TTA_ratio', 'perp_TTP_ratio', 'perp_GTA_ratio'
        """
        if data == None and type_ == None:
            return self.data
    
    # add data

    # Spot books

    def add_binance_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["binance_btcusdt"].update_books(data)

    def add_binance_spot_btcfdusd_depth(self, data):
        self.books["spot"].axis["binance_btcfdusd"].update_books(data)

    def add_okx_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["okx_btcusdt"].update_books(data)

    def add_bybit_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["bybit_btcusdt"].update_books(data)

    def add_bybit_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["bybit_btcusdt"].update_books(data)

    def add_bybit_spot_btcusdc_depth(self, data):
        self.books["spot"].axis["bybit_btcusdc"].update_books(data)

    def add_bingx_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["bingx_btcusdt"].update_books(data)

    def add_mexc_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["mexc_btcusdt"].update_books(data)

    def add_kucoin_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["kucoin_btcusdt"].update_books(data)

    def add_coinbase_spot_btcusd_depth(self, data):
        self.books["spot"].axis["coinbase_btcusd"].update_books(data)

    def add_htx_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["htx_btcusdt"].update_books(data)

    def add_gateio_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["gateio_btcusdt"].update_books(data)

    def add_bitget_spot_btcusdt_depth(self, data):
        self.books["spot"].axis["bitget_btcusdt"].update_books(data)

    # Spot trades

    def add_binance_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["binance_btcusdt"].input_trades(data)

    def add_binance_spot_btcfdusd_trades(self, data):
        self.trades["spot"].axis["binance_btcfdusd"].input_trades(data)

    def add_okx_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["okx_btcusdt"].input_trades(data)

    def add_bybit_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["bybit_btcusdt"].input_trades(data)

    def add_bybit_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["bybit_btcusdt"].input_trades(data)

    def add_bybit_spot_btcusdc_trades(self, data):
        self.trades["spot"].axis["bybit_btcusdc"].input_trades(data)

    def add_bingx_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["bingx_btcusdt"].input_trades(data)

    def add_mexc_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["mexc_btcusdt"].input_trades(data)

    def add_kucoin_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["kucoin_btcusdt"].input_trades(data)

    def add_coinbase_spot_btcusd_trades(self, data):
        self.trades["spot"].axis["coinbase_btcusd"].input_trades(data)

    def add_htx_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["htx_btcusdt"].input_trades(data)

    def add_gateio_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["gateio_btcusdt"].input_trades(data)

    def add_bitget_spot_btcusdt_trades(self, data):
        self.trades["spot"].axis["bitget_btcusdt"].input_trades(data)

    # Perp books
    
    def add_binance_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["binance_btcusdt"].update_books(data) 

    def add_binance_perp_btcusd_depth(self, data):
        self.books["perpetual"].axis["binance_btcusd"].update_books(data) 

    def add_okx_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["okx_btcusdt"].update_books(data) 

    def add_okx_perp_btcusd_depth(self, data):
        self.books["perpetual"].axis["okx_btcusd"].update_books(data)

    def add_bybit_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["bybit_btcusdt"].update_books(data) 

    def add_bybit_perp_btcusd_depth(self, data):
        self.books["perpetual"].axis["bybit_btcusd"].update_books(data)

    def add_bingx_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["bingx_btcusdt"].update_books(data)   

    def add_mexc_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["mexc_btcusdt"].update_books(data) 

    def add_kucoin_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["kucoin_btcusdt"].update_books(data) 

    def add_deribit_perp_btcusd_depth(self, data):
        self.books["perpetual"].axis["deribit_btcusd"].update_books(data) 

    def add_htx_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["htx_btcusdt"].update_books(data) 

    def add_gateio_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["gateio_btcusdt"].update_books(data) 

    def add_bitget_perp_btcusdt_depth(self, data):
        self.books["perpetual"].axis["bitget_btcusdt"].update_books(data) 

    # Perp trades

    def add_binance_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["binance_btcusdt"].input_trades(data) 

    def add_binance_perp_btcusd_trades(self, data):
        self.trades["perpetual"].axis["binance_btcusd"].input_trades(data) 

    def add_okx_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["okx_btcusdt"].input_trades(data) 

    def add_okx_perp_btcusd_trades(self, data):
        self.trades["perpetual"].axis["okx_btcusd"].input_trades(data)

    def add_bybit_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["bybit_btcusdt"].input_trades(data) 

    def add_bybit_perp_btcusd_trades(self, data):
        self.trades["perpetual"].axis["bybit_btcusd"].input_trades(data)

    def add_bingx_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["bingx_btcusdt"].input_trades(data)   

    def add_mexc_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["mexc_btcusdt"].input_trades(data) 

    def add_kucoin_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["kucoin_btcusdt"].input_trades(data) 

    def add_deribit_perp_btcusd_trades(self, data):
        self.trades["perpetual"].axis["deribit_btcusd"].input_trades(data) 

    def add_htx_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["htx_btcusdt"].input_trades(data) 

    def add_gateio_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["gateio_btcusdt"].input_trades(data) 

    def add_bitget_perp_btcusdt_trades(self, data):
        self.trades["perpetual"].axis["bitget_btcusdt"].input_trades(data)         

    # OI

    def add_binance_perp_btcusdt_oi(self, data):
        self.oifunding.axis["binance_btcusdt"].input_oi(data) 

    def add_binance_perp_btcusd_oi(self, data):
        self.oifunding.axis["binance_btcusd"].input_oi(data) 

    def add_okx_perp_btcusdt_oi(self, data):
        self.oifunding.axis["okx_btcusdt"].input_oi(data) 

    def add_okx_perp_btcusd_oi(self, data):
        self.oifunding.axis["okx_btcusd"].input_oi(data) 

    def add_bingx_perp_btcusdt_oi(self, data):
        self.oifunding.axis["bingx_btcusdt"].input_oi(data) 

    def add_gateio_perp_btcusdt_oi(self, data):
        self.oifunding.axis["gateio_btcusdt"].input_oi(data) 

    def add_htx_perp_btcusdt_oi(self, data):
        self.oifunding.axis["htx_btcusdt"].input_oi(data) 
    
    # Funding

    def add_binance_perp_btcusdt_funding(self, data):
        self.oifunding.axis["binance_btcusdt"].input_funding(data) 

    def add_binance_perp_btcusd_funding(self, data):
        self.oifunding.axis["binance_btcusd"].input_funding(data) 

    def add_okx_perp_btcusdt_funding(self, data):
        self.oifunding.axis["okx_btcusdt"].input_funding(data) 

    def add_okx_perp_btcusd_funding(self, data):
        self.oifunding.axis["okx_btcusd"].input_funding(data) 

    def add_bingx_perp_btcusdt_funding(self, data):
        self.oifunding.axis["bingx_btcusdt"].input_funding(data) 

    def add_gateio_perp_btcusdt_funding(self, data):
        self.oifunding.axis["gateio_btcusdt"].input_funding(data) 

    def add_htx_perp_btcusdt_funding(self, data):
        self.oifunding.axis["htx_btcusdt"].input_funding(data) 
    
    # OIFund  

    def add_bybit_perp_btcusdt_oifunding(self, data):
        self.oifunding.axis["bybit_btcusdt"].input_oi_funding(data) 

    def add_bybit_perp_btcusd_oifunding(self, data):
        self.oifunding.axis["bybit_btcusd"].input_oi_funding(data) 

    def add_mexc_perp_btcusdt_oifunding(self, data):
        self.oifunding.axis["mexc_btcusdt"].input_oi_funding(data) 

    def add_kucoin_perp_btcusdt_oifunding(self, data):
        self.oifunding.axis["kucoin_btcusdt"].input_oi_funding(data) 

    def add_bitget_perp_btcusdt_oifunding(self, data):
        self.oifunding.axis["bitget_btcusdt"].input_oi_funding(data) 

    def add_htx_perp_btcusdt_funding(self, data):
        self.oifunding.axis["htx_btcusdt"].input_oi_funding(data) 

    def add_deribit_perp_btcusd_oifunding(self, data):
        self.oifunding.axis["deribit_btcusd"].input_oi_funding(data) 
    
    # Liquidations
    
    def add_binance_perp_btcusdt_liquidations(self, data):
        self.liquidations.axis["binance_btcusdt"].input_liquidations(data) 

    def add_binance_perp_btcusd_liquidations(self, data):
        self.liquidations.axis["binance_btcusd"].input_liquidations(data) 

    def add_okx_perp_btc_liquidations(self, data):
        self.liquidations.axis["okx_btc"].input_liquidations(data) 

    def add_bybit_perp_btcusdt_liquidations(self, data):
        self.liquidations.axis["bybit_btcusdt"].input_liquidations(data) 

    def add_bybit_perp_btcusd_liquidations(self, data):
        self.liquidations.axis["bybit_btcusd"].input_liquidations(data)

    def add_gateio_perp_btcusdt_liquidations(self, data):
        self.liquidations.axis["gateio_btcusdt"].input_liquidations(data)  
    
    # Positions

    def add_binance_perp_btcusdt_tta(self, data):
        self.positionsTTA.axis_ratio["binance_btcusdt_TTA"].input_binance_gta_tta_ttp(data) 

    def add_binance_perp_btcusd_tta(self, data):
        self.positionsTTA.axis_ratio["binance_btcusd_TTA"].input_binance_gta_tta_ttp(data) 

    def add_binance_perp_btcusdt_ttp(self, data):
        self.positionsTTP.axis_ratio["binance_btcusdt_TTP"].input_binance_gta_tta_ttp(data) 

    def add_binance_perp_btcusd_ttp(self, data):
        self.positionsTTP.axis_ratio["binance_btcusd_TTP"].input_binance_gta_tta_ttp(data) 

    def add_binance_perp_btcusdt_gta(self, data):
        self.positionsGTA.axis_ratio["binance_btcusdt_GTA"].input_binance_gta_tta_ttp(data) 

    def add_binance_perp_btcusd_gta(self, data):
        self.positionsGTA.axis_ratio["binance_btcusd_GTA"].input_binance_gta_tta_ttp(data) 

    def add_okx_perp_btc_gta(self, data):
        self.positionsGTA.axis_ratio["okx_btc_GTA"].input_okx_gta(data) 

    def add_bybit_perp_btcusdt_gta(self, data):
        self.positionsGTA.axis_ratio["bybit_btcusdt_GTA"].input_bybit_gta(data) 

    def add_bybit_perp_btcusd_gta(self, data):
        self.positionsGTA.axis_ratio["bybit_btcusd_GTA"].input_bybit_gta(data)

    # Options

    def add_deribit_option_btc_oi(self, data):
        self.optionoi.aggregator.axis["deribit"].input_oi(data)

    def add_bybit_option_btc_oi(self, data):
        self.optionoi.aggregator.axis["bybit"].input_oi(data)

    def add_okx_option_btc_oi(self, data):
        self.optionoi.aggregator.axis["okx"].input_oi(data)

    # Test Synth

    def input_from_json(self, abs_path):
        """
            abs_path : the path to the flolder with json files
            json files must be of the same name as inpu methods withou "add_"
            Ex: if the method is add_okx_option_btc_oi then the file name should be okx_option_btc_oi.json
        """
        callable_methods = [method for method in dir(self) if callable(getattr(self, method)) and not method.startswith("__")]
        methods = []
        methods_str = []
        for method_name in sorted(callable_methods):
            try:
                if method_name.split("_")[1] in self.exchanges_spot_perp and method_name.split("_")[2] != "option":
                    method = getattr(self, method_name)
                    methods.append(method)
                    methods_str.append(method_name)
                if method_name.split("_")[1] in self.exchanges_option and method_name.split("_")[2] not in ["spot", "perp"]:
                    method = getattr(self, method_name)
                    methods.append(method)
                    methods_str.append(method_name)   
            except:
                pass   
        files = ["_".join(x.split("_")[1:]) + ".json" for x in sorted(methods_str)]

        v = {
            x : { 
            "data" : json.load(open("".join([f"{abs_path}/", x]))),
            "lookup" : f,
            } 
            for x, f in zip(files, methods)
            }

        for key in v.keys():
            for d in v[key]["data"]:
                d = json.dumps(d)
                v[key]["lookup"](d)

    def test_empty_dataframes(self):

        total_empty = []

        # spot

        for books, trades in zip(self.books["spot"].axis, self.trades["spot"].axis):
            b = self.books["spot"].axis[books].snapshot
            t = self.trades["spot"].axis[trades].snapshot_total
            try:
                if b.empty:
                    print(f"spot, books, {books}, is empty")
                    total_empty.append(b)
            except:
                print(f"spot, books, {books}, is empty")
                total_empty.append(b)
            try:
                if t.empty:
                    print(f"spot, trades, {trades}, is empty")
                    total_empty.append(b)
            except:
                print(f"spot, trades, {trades}, is empty")
                total_empty.append(b)
        
        # perp

        for books, trades in zip(self.books["perpetual"].axis, self.trades["perpetual"].axis):
            b = self.books["perpetual"].axis[books].snapshot
            t = self.trades["perpetual"].axis[trades].snapshot_total
            try:
                if b.empty:
                    print(f"perpetual, books, {books}, is empty")
                    total_empty.append(b)
            except:
                print(f"perpetual, books, {books}, is empty")
                total_empty.append(b)
            try:
                if t.empty:
                    print(f"perpetual, trades, {trades}, is empty")
                    total_empty.append(b)
            except:
                print(f"perpetual, trades, {trades}, is empty")
                total_empty.append(b)

        # oif

        for oiff in self.oifunding.axis:
            oif = self.oifunding.axis[oiff].snapshot
            try:
                if len(oif) != 0:
                    pass
                else:
                    print(f"perpetual, oif, {oiff}, is empty")
                    total_empty.append(p)
            except:
                print(f"perpetual, oif, {oiff}, is empty")
                total_empty.append(b)


        # Liquidations

        for liqss in self.liquidations.axis:
            liqs = self.liquidations.axis[liqss].snapshot_total
            try:
                if liqs.empty:
                    print(f"perpetual, liquidations, {liqss}, is empty")
                    total_empty.append(b)
            except:
                print(f"perpetual, liquidations, {liqss}, is empty")
                total_empty.append(b)

        # Positions
        
        for TTA in self.positionsTTA.axis_ratio:
            r = self.positionsTTA.axis_ratio[TTA].data
            if len(r) != 0:
                pass
            else:
                print(f"perpetual, TTA, {TTA}, is empty")
                total_empty.append(TTA)
        
        for TTP in self.positionsTTP.axis_ratio:
            r = self.positionsTTP.axis_ratio[TTP].data
            if len(r) != 0:
                pass
            else:
                print(f"perpetual, TTP, {TTP}, is empty")
                total_empty.append(TTP)

        for GTA in self.positionsGTA.axis_ratio:
            r = self.positionsGTA.axis_ratio[GTA].data
            if len(r) != 0:
                pass
            else:
                print(f"perpetual, GTA, {GTA}, is empty")
                total_empty.append(GTA)

        if len(total_empty) == 0:
            print("No empty frames")
        
        # return total_empty
    
    def test_display_books(self, insType, exinstrument):
        """
            insType : spot. perpetual
            exinstrument : Ex. gateio_btcusdt
        """
        return self.books[insType].axis[exinstrument].df

    def test_display_trades(self, insType, exinstrument):
        """
            insType : spot. perpetual
            exinstrument : Ex. gateio_btcusdt
        """
        print(self.trades[insType].axis[exinstrument].buys)
        if self.trades[insType].axis[exinstrument].buys.empty:
            print(self.trades[insType].axis[exinstrument].sells)

    def test_display_oif(self, exinstrument):
        """
            exinstrument : Ex. gateio_btcusdt
        """
        print(self.oifunding.axis[exinstrument].raw_data)

    def test_display_liquidations(self, exinstrument):
        """
            exinstrument : Ex. gateio_btcusdt
        """
        print(self.liquidations.axis[exinstrument].longs)
        if self.liquidations.axis[exinstrument].longs.empty:
            print(self.liquidations.axis[exinstrument].shorts)

    def test_display_gta(self, exinstrument):
        """
            exinstrument : Ex. binance_btcusdt
        """
        print("Ratios : ", self.positionsGTA.axis_ratio[exinstrument].data)

    def test_display_tta(self, exinstrument):
        """
            exinstrument : Ex. binance_btcusdt
        """
        print("Ratios : ", self.positionsTTA.axis_ratio[exinstrument].data)
    def test_display_ttp(self, exinstrument):
        """
            exinstrument : Ex. binance_btcusdt
        """
        print("Ratios : ", self.positionsTTP.axis_ratio[exinstrument].data)
    
    def test_display_data(self, what):
        """
            what : 'books_spot', 'books_perpetual', 'trades_spot', 'trades_perpetual', 'adjustmests_spot', 'adjustmests_perpetual', 'oifunding_perpetual', 'liquidations_perpetual', 'TTA_perpetual', 'TTP_perpetual', 'GTA_perpetual', 'oi_option'
            IF the distribution has tails, you had miscalculations .
        """
        data = self.data.get(what)
        df = pd.DataFrame(list(data.items()), columns=['Category', 'Value'])
        df.plot(kind='bar', x='Category', y='Value', legend=False)
        plt.title('Distribution Plot')
        plt.xlabel('Category')
        plt.ylabel('Value')
        plt.show()

    def display_full_data(self):
        """
            Make sure to have more than 60 seconds of the data and the most recent data
        """

        list_spot_instruments = []
        for exchnage in self.axis:
            for instrument in self.axis[exchnage].spot_axis["books"]:
                list_spot_instruments.append(f"{exchnage}_{instrument}")
        list_perp_instruments = []
        for exchnage in self.axis:
            for instrument in self.axis[exchnage].perpetual_axis["books"]:
                list_perp_instruments.append(f"{exchnage}_{instrument}")

        graphs = {}

        for exinstrument in list_spot_instruments:
            b = self.books["spot"].axis[exinstrument].df
            for index, row in b.iterrows():
                if not all(row == 0):
                    break
            graphs["_".join([exinstrument, "Depth_Spot"])] = row

        for exinstrument in list_perp_instruments:
            b = self.books["perpetual"].axis[exinstrument].df
            for index, row in b.iterrows():
                if not all(row == 0):
                    break
            graphs["_".join([exinstrument, "Depth_Perpetual"])] = row

        for exinstrument in list_spot_instruments:
            b = self.trades["spot"].axis[exinstrument].buys.iloc[:, 1:].sum()
            if b.sum() != 0:
                graphs["_".join([exinstrument, "Trades_Spot"])] = b
            else:
                b = self.trades["spot"].axis[exinstrument].sells.iloc[:, 1:].sum()
                graphs["_".join([exinstrument, "Trades_Spot"])] = b

        for exinstrument in list_perp_instruments:
            b = self.trades["perpetual"].axis[exinstrument].buys.iloc[:, 1:].sum()
            if b.sum() != 0:
                graphs["_".join([exinstrument, "Trades_Perpetual"])] = b
            else:
                b = self.trades["perpetual"].axis[exinstrument].sells.iloc[:, 1:].sum()
                graphs["_".join([exinstrument, "Trades_Perpetual"])] = b

        for instype in ["spot", "perpetual"]:
            b = self.adjustmests[instype].data["voids"]
            b = pd.Series(b)
            graphs["_".join([instype, f"Voids"])] = b
            b = self.adjustmests[instype].data["reinforces"]
            b = pd.Series(b)
            graphs["_".join([instype, f"reinforces"])] = b

        graphs["Agg_Books_Perpetual"] = pd.Series(self.data["perp_books"])
        graphs["Agg_Books_Spot"] = pd.Series(self.data["spot_books"])
        graphs["Agg_Trades_Spot"] = pd.Series(self.data["spot_VolProfile"])
        graphs["Agg_Trades_Perpetual"] = pd.Series(self.data["perp_VolProfile"])

        for exinstrument in list_perp_instruments:
            b = self.oifunding.axis[exinstrument].raw_data.iloc[:, 3:].sum()
            if b.sum() != 0:
                graphs["_".join([exinstrument, "OI Profile"])] = b
            else:
                try:
                    b = self.oifunding.axis[exinstrument].snapshot.iloc[:, 3:].sum()
                    graphs["_".join([exinstrument, "OI Profile"])] = b
                except:
                    pass
        graphs["Agg_OI_Turnover"] = pd.Series(self.data["perp_oi_turnover"])
        graphs["OIs_per_instrument"] = pd.Series(self.data["perp_OIs_per_instrument"])
        graphs["Fundings_per_instrument"] = pd.Series(self.data["perp_fundings_per_instrument"]).dropna()

        for instrument in [x for x in list_perp_instruments if x in self.liquidations.axis.keys()]:
            try:
                l = self.liquidations.axis[instrument].longs.iloc[:, 1:].sum()
                if not l.empty:
                    graphs["_".join([instrument, "long_liquidations"])] = l
                else:
                    print("WARNING, ", instrument, " has no long liquidations")
            except:
                print("WARNING, ", instrument, " has no long liquidations")
            try:
                s = self.liquidations.axis[instrument].shorts.iloc[:, 1:].sum()
                if not s.empty:
                    graphs["_".join([instrument, "short_liquidations"])] = s
                else:
                    print("WARNING, ", instrument, " has no short liquidations")
            except:
                print("WARNING, ", instrument, " has no short liquidations")

        graphs["Total_Short_Liquidations"] = pd.Series(self.data.get("perp_liquidations_shorts"))
        graphs["Total_Long_Liquidations"] = pd.Series(self.data.get("perp_liquidations_longs"))

        graphs["GTA_per_instrument"] = pd.Series(self.positionsGTA.ratios)
        graphs["TTA_per_instrument"] = pd.Series(self.positionsTTA.ratios)
        graphs["TTP_per_instrument"] = pd.Series(self.positionsTTP.ratios)

        for exchange in self.optionoi.aggregator.axis:
            option_object = self.optionoi.aggregator.axis[exchange]
            for expiration in option_object.df_call:
                try:
                    df = self.optionoi.aggregator.axis["bybit"].df_call[expiration].transpose()
                    df = pd.Series(df.values.transpose()[0], index=df.index.values)
                except:
                    print(f"WARNING, {exchange} option expiration {expiration} of puts is empty")
                    pass
                graphs["_".join([exchange, "Puts", expiration])] = df
            for expiration in option_object.df_put:
                try:
                    df = self.optionoi.aggregator.axis["bybit"].df_put[expiration].transpose()
                    df = pd.Series(df.values.transpose()[0], index=df.index.values)
                    graphs["_".join([exchange, "Calls", expiration])] = df
                except:
                    print(f"WARNING, {exchange} option expiration {expiration} of calls is empty")
                    pass

        for key in self.data:
            if "oi_option" in key:
                graphs[key] = pd.Series(self.data.get(key))
            else:
                pass

        num_plots = len(graphs)
        num_cols = min(4, num_plots)  
        num_rows = -(-num_plots // num_cols)  
        fig, axes = plt.subplots(nrows=num_rows, ncols=num_cols, figsize=(15, num_rows * 5))
        for (series_name, series_values), ax in zip(graphs.items(), axes.flatten()):
            try:
                non_zero_indexes = series_values[series_values > 0].index.astype(float)
                ax.set_xlim([min(non_zero_indexes), max(non_zero_indexes)])
            except:
                pass
            try:
                non_zero_values = series_values[series_values > 0].values
                ax.set_ylim([min(non_zero_values), max(non_zero_values)])
            except:
                pass
            try:
                series_values.plot(kind='bar', ax=ax)
            except:
                print(series_values)
            ax.set_title(f'{series_name}')
            ax.set_xlabel('Levels')
            ax.set_ylabel('BTC amount')
            try:
                length_ticks = len(ax.get_xticks())
                if length_ticks > 20:
                    xticks_pos = ax.get_xticks()[::int(length_ticks/5)]
                    ax.set_xticks(xticks_pos)
            except:
                pass

        plt.tight_layout()
        plt.show()



    def flatten_data(self, d, sep='_'):
        """
            Formats the data of the dictionary
        """

        def rename_keys(original_dict, key_mapping):
            renamed_dict = {}
            for old_key, new_key in key_mapping.items():
                if old_key in original_dict:
                    renamed_dict[new_key] = original_dict[old_key]
            return renamed_dict

        def clean_dict(original_dict):
            # original_dict = {k: v for k, v in copy(original_dict).items() if not np.isnan(v)}
            df = pd.Series(original_dict).dropna()
            original_dict = df.to_dict()
            original_dict = {k: v for k, v in original_dict.items() if v != 0}
            return original_dict

        def clean_list(original_list):
            original_list = [item for item in copy(original_list) if not np.isnan(item) ]
            original_list = [item for item in original_list if item != 0]
            return original_list


        flattened_dict = {}
        for primary_key, data_level_2 in d.items():
            if "books" in primary_key:
                flattened_dict["_".join(primary_key.split("_")[::-1])] = data_level_2["books"]
            # if "trades" in primary_key:
            #     for trades_key in data_level_2:
            else:
                for trades_key in data_level_2:
                    flattened_dict[f"{primary_key.replace('trades_', '')}{sep}{trades_key}"] = data_level_2[trades_key]

        # Deal with options data
        target_keys = ['oi_option_puts', 'oi_option_calls']
        for target_key in target_keys:
            for option_key in flattened_dict.get(target_key):
                    new_keyy = f"{target_key}_{option_key}"
                    flattened_dict[new_keyy] = flattened_dict.get(target_key).get(option_key)
        for key in ['oi_option_puts', 'oi_option_calls']:
            del flattened_dict[key]

        key_mapping = {}

        for key in flattened_dict:
            old_key = key
            if "Std" in key:
                key = key.replace("Std", "Vola")
            if "std" in key:
                key = key.replace("std", "Vola")
            if "Volume" in key:
                key = key.replace("Volume", "Vol")
            if "volume" in key:
                key = key.replace("volume", "Vol")
            if "perpetual" in key:
                key = key.replace("perpetual", "perp")
            if "adjustmests" in key:
                key = key.replace("adjustmests", "")
            if "oifunding" in key:
                key = key.replace("oifunding", "")
            if "timestamp" in key:
                key = "timestamp"
            if "volatility" in key:
                key = key.replace("volatility", "Vola")
            if "Liquidations" in key:
                key = key.replace("Liquidations", "")
            key_mapping[old_key] = key

        key_mapping_2 = {
            "spot_buyTrades" : "spot_orderedBuyTrades", "spot_sellTrades" : "spot_orderedSellTrades",
            "perp_buyTrades" : "perp_orderedBuyTrades", "perp_sellTrades" : "perp_orderedSellTrades",
            "_perp_ois" : "perp_orderedOIChanges", "perp_liquidations_shorts" : "perp_liquidations_orderedShorts",
            "perp_liquidations_longs" : "perp_liquidations_orderedLongs",
            }
            
        keys_to_remove = [
            "_spot_voidsVola", "_spot_reinforcesVola",  "_spot_totalVoidsVola.",
            "_spot_totalReinforcesVola", 
            "_spot_price", "_perp_price",
            "_perp_voidsVola", "_perp_reinforcesVola",  "_perp_totalVoidsVola.",
            "_perp_totalReinforcesVola", "liquidations_perp_price",
            ]


        flattened_dict = rename_keys(flattened_dict, key_mapping)
        flattened_dict = {key_mapping_2[k] if k in key_mapping_2 else k: v for k, v in flattened_dict.items()}
        flattened_dict = {k : i for k, i in flattened_dict.items() if k not in keys_to_remove }

        for key in copy(flattened_dict):
            new_key_list = key.split("_")
            if  new_key_list[0] in ["liquidation", "liquidations", "TTA", "GTA", "TTP"]:
                flattened_dict["_".join([new_key_list[1], new_key_list[0], new_key_list[2]])] = flattened_dict[key]

        flattened_dict = {k[1:] if k.startswith("_") else k: v for k, v in flattened_dict.items()}

        for key, value in copy(flattened_dict).items():
            if isinstance(value, dict):
                flattened_dict[key] = clean_dict(value)
            if isinstance(value, list):
                flattened_dict[key] = clean_list(value)

        
        return flattened_dict