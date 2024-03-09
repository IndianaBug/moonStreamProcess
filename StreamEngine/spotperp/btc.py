import sys
import time
import json
from os.path import abspath, dirname
from IPython.display import display, clear_output

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import flow
from lookups import btc as btc_lookups_btc
from lookups import unit_conversion_btc
from frametest import tester

lookups_btc = btc_lookups_btc(unit_conversion_btc)

class binance_flow(tester):

    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_depth_lookup, book_ceil_thresh),
                "btcfdusd" : flow.booksflow('binance', 'btc_fdusd', 'perpetual', level_size, lookups_btc.binance_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_trades_lookup),
                "btcfdusd" : flow.tradesflow('binance', 'btc_fdusd', 'perpetual', level_size, lookups_btc.binance_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_depth_lookup, book_ceil_thresh),
                "btcusd" : flow.booksflow('binance', 'btc_usd', 'perpetual', level_size, lookups_btc.binance_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_trades_lookup),
                "btcusd" : flow.tradesflow('binance', 'btc_usd', 'perpetual', level_size, lookups_btc.binance_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_OI_lookup, lookups_btc.binance_funding_lookup),
                "btcusd" : flow.oiFundingflow('binance', 'btc_usd', 'perpetual', level_size, lookups_btc.binance_OI_lookup, lookups_btc.binance_funding_lookup),
            },
            "liquidations" : {
                "btcusdt" : flow.liquidationsflow('binance', 'btc_usdt', 'perpetual', level_size, lookups_btc.binance_liquidations_lookup),
                "btcusd" : flow.liquidationsflow('binance', 'btc_usd', 'perpetual', level_size, lookups_btc.binance_liquidations_lookup),
            },
            "indicators" : {
                "btcusdt_TTA" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "TTA", lookups_btc.binance_GTA_TTA_TTP_lookup),
                "btcusdt_TTP" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "TTP", lookups_btc.binance_GTA_TTA_TTP_lookup),
                "btcusdt_GTA" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "GTA", lookups_btc.binance_GTA_TTA_TTP_lookup),
                "btcusd_TTA" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "TTA", lookups_btc.binance_GTA_TTA_TTP_lookup),
                "btcusd_TTP" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "TTP", lookups_btc.binance_GTA_TTA_TTP_lookup),
                "btcusd_GTA" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "GTA", lookups_btc.binance_GTA_TTA_TTP_lookup),
            }
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_d_spot_fdusd(self, data):
        self.spot_axis["books"]["btcfdusd"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_t_spot_fdusd(self, data):
        self.spot_axis["trades"]["btcfdusd"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_d_perp_usd(self, data):
        self.perpetual_axis["books"]["btcusd"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_t_perp_usd(self, data):
        self.perpetual_axis["trades"]["btcusd"].input_trades(data)

    def add_oi_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi(data)

    def add_f_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_funding(data)

    def add_oi_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_oi(data)

    def add_f_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_funding(data)

    def add_l_usdt(self, data):
        self.perpetual_axis["liquidations"]["btcusdt"].input_liquidations(data)

    def add_l_usd(self, data):
        self.perpetual_axis["liquidations"]["btcusd"].input_liquidations(data)

    def add_tta_usdt(self, data):
        self.perpetual_axis["indicators"]["btcusdt_TTA"].input_binance_gta_tta_ttp(data)

    def add_tta_usd(self, data):
        self.perpetual_axis["indicators"]["btcusd_TTA"].input_binance_gta_tta_ttp(data)

    def add_ttp_usdt(self, data):
        self.perpetual_axis["indicators"]["btcusdt_TTP"].input_binance_gta_tta_ttp(data)

    def add_ttp_usd(self, data):
        self.perpetual_axis["indicators"]["btcusd_TTP"].input_binance_gta_tta_ttp(data)

    def add_gta_usdt(self, data):
        self.perpetual_axis["indicators"]["btcusdt_GTA"].input_binance_gta_tta_ttp(data)

    def add_gta_usd(self, data):
        self.perpetual_axis["indicators"]["btcusd_GTA"].input_binance_gta_tta_ttp(data)


    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "binance_btcusdt_spot_depth", "binance_btcfdusd_spot_depth", "binance_btcusdt_spot_trades", "binance_btcfdusd_spot_trades",
            "binance_btcusdt_perpetual_depth", "binance_btcusd_perpetual_depth", "binance_btcusdt_perpetual_trades", "binance_btcusd_perpetual_trades",
            "binance_btcusdt_perpetual_fundingRate", "binance_btcusd_perpetual_fundingRate", "binance_btcusdt_perpetual_OI", "binance_btcusd_perpetual_OI",
            "binance_btcusdt_perpetual_liquidations", "binance_btcusd_perpetual_liquidations", "binance_btcusdt_perpetual_TTA", "binance_btcusd_perpetual_TTA",
            "binance_btcusdt_perpetual_TTP", "binance_btcusd_perpetual_TTP", "binance_btcusdt_perpetual_GTA", "binance_btcusd_perpetual_GTA",
            ]

        functions_list = [
            self.add_d_spot_usdt, self.add_d_spot_fdusd, self.add_t_spot_usdt, self.add_t_spot_fdusd,
            self.add_d_perp_usdt, self.add_d_perp_usd, self.add_t_perp_usdt, self.add_t_perp_usd,
            self.add_f_usdt, self.add_f_usd, self.add_oi_usdt, self.add_oi_usd,
            self.add_l_usdt, self.add_l_usd, self.add_tta_usdt, self.add_tta_usd,
            self.add_ttp_usdt, self.add_ttp_usd, self.add_gta_usdt, self.add_gta_usd
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



class okx_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('okx', 'btc_usdt', 'spot', level_size, lookups_btc.okx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('okx', 'btc_usdt', 'spot', level_size, lookups_btc.okx_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('okx', 'btc_usdt', 'perpetual', level_size, lookups_btc.okx_depth_lookup, book_ceil_thresh),
                "btcusd" : flow.booksflow('okx', 'btc_usd', 'perpetual', level_size, lookups_btc.okx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('okx', 'btc_usdt', 'perpetual', level_size, lookups_btc.okx_trades_lookup),
                "btcusd" : flow.tradesflow('okx', 'btc_usd', 'perpetual', level_size, lookups_btc.okx_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('okx', 'btc_usdt', 'perpetual', level_size, lookups_btc.okx_OI_lookup, lookups_btc.okx_funding_lookup),
                "btcusd" : flow.oiFundingflow('okx', 'btc_usd', 'perpetual', level_size, lookups_btc.okx_OI_lookup, lookups_btc.okx_funding_lookup),
            },
            "liquidations" : {
                "btc" : flow.liquidationsflow('okx', 'btc_usdt', 'perpetual', level_size, lookups_btc.okx_liquidations_lookup),
            },
            "indicators" : {
                "btc_GTA" : flow.indicatorflow('okx', 'btc', 'perpetual', "TTA", lookups_btc.okx_GTA_lookup),
            }
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_d_perp_usd(self, data):
        self.perpetual_axis["books"]["btcusd"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_t_perp_usd(self, data):
        self.perpetual_axis["trades"]["btcusd"].input_trades(data)

    def add_oi_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi(data)

    def add_f_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_funding(data)

    def add_oi_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_oi(data)

    def add_f_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_funding(data)

    def add_l(self, data):
        self.perpetual_axis["liquidations"]["btc"].input_liquidations(data)

    def add_gta(self, data):
        self.perpetual_axis["indicators"]["btc_GTA"].input_okx_gta(data)


    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "okx_btcusdt_spot_depth", "okx_btcusdt_spot_trades", 
            "okx_btcusdt_perpetual_depth", "okx_btcusd_perpetual_depth", "okx_btcusdt_perpetual_trades", "okx_btcusd_perpetual_trades",
            "okx_btcusdt_perpetual_fundingRate", "okx_btcusd_perpetual_fundingRate", "okx_btcusdt_perpetual_OI", "okx_btcusd_perpetual_OI",
            "okx_btcusdt_perpetual_liquidations", "okx_btcusd_perpetual_GTA",
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt, 
            self.add_d_perp_usdt, self.add_d_perp_usd, self.add_t_perp_usdt, self.add_t_perp_usd,
            self.add_f_usdt, self.add_f_usd, self.add_oi_usdt, self.add_oi_usd,
            self.add_l, self.add_gta
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


class bybit_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_depth_lookup, book_ceil_thresh),
                "btcusdc" : flow.booksflow('bybit', 'btc_usdc', 'perpetual', level_size, lookups_btc.bybit_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_trades_lookup),
                "btcusdc" : flow.tradesflow('bybit', 'btc_usdc', 'perpetual', level_size, lookups_btc.bybit_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_depth_lookup, book_ceil_thresh),
                "btcusd" : flow.booksflow('bybit', 'btc_usd', 'perpetual', level_size, lookups_btc.bybit_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_trades_lookup),
                "btcusd" : flow.tradesflow('bybit', 'btc_usd', 'perpetual', level_size, lookups_btc.bybit_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_OI_funding_lookup),
                "btcusd" : flow.oiFundingflow('bybit', 'btc_usd', 'perpetual', level_size, lookups_btc.bybit_OI_funding_lookup),
            },
            "liquidations" : {
                "btcusdt" : flow.liquidationsflow('bybit', 'btc_usdt', 'perpetual', level_size, lookups_btc.bybit_liquidations_lookup),
                "btcusd" : flow.liquidationsflow('bybit', 'btc_usd', 'perpetual', level_size, lookups_btc.bybit_liquidations_lookup),
            },
            "indicators" : {
                "btcusdt_GTA" : flow.indicatorflow('bybit', 'btc_usdt', 'perpetual', "GTA", lookups_btc.bybit_GTA_lookup),
                "btcusd_GTA" : flow.indicatorflow('bybit', 'btc_usd', 'perpetual', "GTA", lookups_btc.bybit_GTA_lookup)
            }
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_d_spot_usdc(self, data):
        self.spot_axis["books"]["btcusdc"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_t_spot_usdc(self, data):
        self.spot_axis["trades"]["btcusdc"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_d_perp_usd(self, data):
        self.perpetual_axis["books"]["btcusd"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_t_perp_usd(self, data):
        self.perpetual_axis["trades"]["btcusd"].input_trades(data)

    def add_oif_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi_funding(data)

    def add_oif_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_oi_funding(data)

    def add_l_usdt(self, data):
        self.perpetual_axis["liquidations"]["btcusdt"].input_liquidations(data)

    def add_l_usd(self, data):
        self.perpetual_axis["liquidations"]["btcusd"].input_liquidations(data)

    def add_gta_usdt(self, data):
        self.perpetual_axis["indicators"]["btcusdt_GTA"].input_bybit_gta(data)

    def add_gta_usd(self, data):
        self.perpetual_axis["indicators"]["btcusd_GTA"].input_bybit_gta(data)

    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "bybit_btcusdt_spot_depth", "bybit_btcusdc_spot_depth", "bybit_btcusdt_spot_trades", "bybit_btcusdc_spot_trades",
            "bybit_btcusdt_perpetual_depth", "bybit_btcusd_perpetual_depth", "bybit_btcusdt_perpetual_trades", "bybit_btcusd_perpetual_trades_1",
            "bybit_btcusdt_perpetual_fundingRate_OI", "bybit_btcusd_perpetual_fundingRate_OI", 
            "bybit_btcusdt_perpetual_liquidations", "bybit_btcusd_perpetual_liquidations", "bybit_btcusdt_perpetual_GTA", "bybit_btcusd_perpetual_GTA",
            ]

        functions_list = [
            self.add_d_spot_usdt, self.add_d_spot_usdc, self.add_t_spot_usdt, self.add_t_spot_usdc,
            self.add_d_perp_usdt, self.add_d_perp_usd, self.add_t_perp_usdt, self.add_t_perp_usd,
            self.add_oif_usdt, self.add_oif_usd, 
            self.add_l_usdt, self.add_l_usd, self.add_gta_usdt, self.add_gta_usd,
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


class bingx_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.bingx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.bingx_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.bingx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.bingx_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.bingx_OI_lookup, lookups_btc.bingx_funding_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_oi_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi(data)

    def add_f_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_funding(data)

    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "bingx_btcusdt_spot_depth", "bingx_btcusdt_spot_trades", 
            "bingx_btcusdt_perpetual_depth", "bingx_btcusdt_perpetual_trades", 
            "bingx_btcusdt_perpetual_fundingRate", "bingx_btcusdt_perpetual_OI", 
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.add_f_usdt,  self.add_oi_usdt, 
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




class coinbase_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusd" : flow.booksflow('coinbase', 'btc_usd', 'spot', level_size, lookups_btc.coinbase_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusd" : flow.tradesflow('coinbase', 'btc_usd', 'spot', level_size, lookups_btc.coinbase_trades_lookup),
            },
        }
        self.perpetual_axis = {"books" : {}}

    def add_d_spot_usd(self, data):
        self.spot_axis["books"]["btcusd"].update_books(data)

    def add_t_spot_usd(self, data):
        self.spot_axis["trades"]["btcusd"].input_trades(data)

    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "coinbase_btcusd_spot_depth", "coinbase_btcusd_spot_trades", 
            ]

        functions_list = [
            self.add_d_spot_usd,  self.add_t_spot_usd,
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




class deribit_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {"books" : {}}
        self.perpetual_axis= {
            "books" : {
                "btcusd" : flow.booksflow('deribit', 'btc_usd', 'perpetual', level_size, lookups_btc.deribit_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusd" : flow.tradesflow('deribit', 'btc_usd', 'perpetual', level_size, lookups_btc.deribit_trades_lookup),
            },
            "oifunding" : {
                "btcusd" : flow.oiFundingflow('deribit', 'btc_usd', 'perpetual', level_size, lookups_btc.deribit_OI_funding_lookup),
            },
        }


    def add_d_perp_usd(self, data):
        self.perpetual_axis["books"]["btcusd"].update_books(data)

    def add_t_perp_usd(self, data):
        self.perpetual_axis["trades"]["btcusd"].input_trades(data)

    def add_oif_usd(self, data):
        self.perpetual_axis["oifunding"]["btcusd"].input_oi_funding(data)


    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "deribit_btcusd_perpetual_depth", "deribit_btcusd_perpetual_trades", 
            "deribit_btcusd_perpetual_fundingOI"
            ]

        functions_list = [
            self.add_d_perp_usd,  self.add_t_perp_usd,
            self.add_oif_usd, 
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




class gateio_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('gateio', 'btc_usdt', 'spot', level_size, lookups_btc.gateio_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('gateio', 'btc_usdt', 'spot', level_size, lookups_btc.gateio_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('gateio', 'btc_usdt', 'perpetual', level_size, lookups_btc.gateio_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('gateio', 'btc_usdt', 'perpetual', level_size, lookups_btc.gateio_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('gateio', 'btc_usdt', 'perpetual', level_size, lookups_btc.gateio_OI_lookup, lookups_btc.gateio_funding_lookup),
            },
            "liquidations" : {
                "btcusdt" : flow.liquidationsflow('gateio', 'btc_usdt', 'perpetual', level_size, lookups_btc.gateio_liquidations_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_oi_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi(data)

    def add_f_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_funding(data)

    def add_l(self, data):
        self.perpetual_axis["liquidations"]["btcusdt"].input_liquidations(data)

    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
            In perovided json files gateio has little data of OI, trades, depth perp, but it works
        """
        files = [
            "gateio_btcusdt_spot_depth", "gateio_btcusdt_spot_trades", 
            "gateio_btcusdt_perpetual_depth", "gateio_btcusdt_perpetual_trades", 
            "gateio_btcusdt_perpetual_fundingRate", "gateio_btcusdt_perpetual_OI", 
            "gateio_btcusdt_perpetual_liquidations"
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.add_f_usdt,  self.add_oi_usdt, 
            self.add_l
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



class bitget_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.bitget_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.bitget_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.bitget_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.bitget_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.bitget_OI_funding_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def input_oif(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi_funding(data)
    
    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
            pepr depth from provided json files works, insufficient data in json file
        """
        files = [
            "bitget_btcusdt_spot_depth", "bitget_btcusdt_spot_trades", 
            "bitget_btcusdt_perpetual_depth", "bitget_btcusdt_perpetual_trades", 
            "bitget_btcusdt_perpetual_fundingOI",
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.input_oif,  
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




class kucoin_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.kucoin_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.kucoin_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.kucoin_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.kucoin_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.kucoin_OI_funding_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def input_oif(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi_funding(data)
    
    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
            pepr depth from provided json files works, insufficient data in json file
        """
        files = [
            "kucoin_btcusdt_spot_depth", "kucoin_btcusdt_spot_trades", 
            "kucoin_btcusdt_perpetual_depth", "kucoin_btcusdt_perpetual_trades", 
            "kucoin_btcusdt_perpetual_fundingOI",
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.input_oif,  
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



class mexc_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.mexc_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.mexc_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.mexc_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.mexc_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bitget', 'btc_usdt', 'perpetual', level_size, lookups_btc.mexc_OI_funding_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def input_oif(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi_funding(data)
    
    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
            pepr depth from provided json files works, insufficient data in json file
        """
        files = [
            "mexc_btcusdt_spot_depth", "mexc_btcusdt_spot_trades", 
            "mexc_btcusdt_perpetual_depth", "mexc_btcusdt_perpetual_trades", 
            "mexc_btcusdt_perpetual_fundingOI",
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.input_oif,  
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


class htx_flow(tester):
    
    def __init__ (self, level_size, book_ceil_thresh):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.htx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.htx_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.htx_depth_lookup, book_ceil_thresh),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.htx_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('bingx', 'btc_usdt', 'perpetual', level_size, lookups_btc.htx_OI_lookup, lookups_btc.htx_funding_lookup),
            },
        }

    def add_d_spot_usdt(self, data):
        self.spot_axis["books"]["btcusdt"].update_books(data)

    def add_t_spot_usdt(self, data):
        self.spot_axis["trades"]["btcusdt"].input_trades(data)

    def add_d_perp_usdt(self, data):
        self.perpetual_axis["books"]["btcusdt"].update_books(data)

    def add_t_perp_usdt(self, data):
        self.perpetual_axis["trades"]["btcusdt"].input_trades(data)

    def add_oi_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_oi(data)

    def add_f_usdt(self, data):
        self.perpetual_axis["oifunding"]["btcusdt"].input_funding(data)

    # Testing
    
    @classmethod
    def create_class(cls, level_size=20, book_ceil_thresh=5):
        return cls(level_size, book_ceil_thresh)

    def input_from_json(self):
        """
            json files should be in the directory data/
            Add your own logic
        """
        files = [
            "htx_btcusdt_spot_depth", "htx_btcusdt_spot_trades", 
            "htx_btcusdt_perpetual_depth", "htx_btcusdt_perpetual_trades", 
            "htx_btcusdt_perpetual_fundingRate", "htx_btcusdt_perpetual_OI", 
            ]

        functions_list = [
            self.add_d_spot_usdt,  self.add_t_spot_usdt,
            self.add_d_perp_usdt,  self.add_t_perp_usdt, 
            self.add_f_usdt,  self.add_oi_usdt, 
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