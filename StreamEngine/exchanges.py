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



class binance_flow():
    
    def __init__ (self, level_size):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : flow.booksflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_depth_lookup),
                "btcfdusd" : flow.booksflow('binance', 'btc_fdusd', 'perpetual', level_size, lookups.binance_depth_lookup),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_trades_lookup),
                "btcfdusd" : flow.tradesflow('binance', 'btc_fdusd', 'perpetual', level_size, lookups.binance_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : flow.booksflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_depth_lookup),
                "btcusd" : flow.booksflow('binance', 'btc_usd', 'perpetual', level_size, lookups.binance_depth_lookup),
            },
            "trades": {
                "btcusdt" : flow.tradesflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_trades_lookup),
                "btcusd" : flow.tradesflow('binance', 'btc_usd', 'perpetual', level_size, lookups.binance_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : flow.oiFundingflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_OI_lookup, lookups.binance_funding_lookup),
                "btcusd" : flow.oiFundingflow('binance', 'btc_usd', 'perpetual', level_size, lookups.binance_OI_lookup, lookups.binance_funding_lookup),
            },
            "liquidations" : {
                "btcusdt" : flow.liquidationsflow('binance', 'btc_usdt', 'perpetual', level_size, lookups.binance_liquidations_lookup),
                "btcusd" : flow.liquidationsflow('binance', 'btc_usd', 'perpetual', level_size, lookups.binance_liquidations_lookup),
            },
            "indicators" : {
                "btcusdt_TTA" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "TTA", lookups.binance_GTA_TTA_TTP_lookup),
                "btcusdt_TTP" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "TTP", lookups.binance_GTA_TTA_TTP_lookup),
                "btcusdt_GTA" : flow.indicatorflow('binance', 'btc_usdt', 'perpetual', "GTA", lookups.binance_GTA_TTA_TTP_lookup),
                "btcusd_TTA" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "TTA", lookups.binance_GTA_TTA_TTP_lookup),
                "btcusd_TTP" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "TTP", lookups.binance_GTA_TTA_TTP_lookup),
                "btcusd_GTA" : flow.indicatorflow('binance', 'btc_usd', 'perpetual', "GTA", lookups.binance_GTA_TTA_TTP_lookup),
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
    def create_class(cls, level_size=20):
        return cls(level_size)

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


    def test_empty_dataframes(self):
        """
            My json files liquidaitons is empty because there is no data, better try with trades
        """

        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                if flow == "indicators":
                    if not bool( self.spot_axis[flow][instrument].data):
                        print(f"spot_axis, {flow}, {instrument}, is empty")


        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                    
                if flow == "indicators":
                    if not bool( self.perpetual_axis[flow][instrument].data):
                        print(f"perpetual_axis, {flow}, {instrument}, is empty")


    def test_dataframes(self):
        # Observe snapshots for any discrepancy
        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.spot_axis[flow][instrument].data
                    if len(s) != 0:
                        print(s)
                time.sleep(5)
                clear_output(wait=True)

        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.perpetual_axis[flow][instrument].data
                    if not bool(s):
                        print(s)
                time.sleep(5)
                clear_output(wait=True)




class okx_flow():
    
    def __init__ (self, level_size):
        self.level_size = level_size
        self.spot_axis = {
            "books" : {
                "btcusdt" : booksflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_depth_lookup),
            },
            "trades": {
                "btcusdt" : tradesflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_trades_lookup),
            },
        }
        self.perpetual_axis= {
            "books" : {
                "btcusdt" : booksflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_depth_lookup),
                "btcusd" : booksflow('okx', 'btc_usd', 'perpetual', level_size, lookups.okx_depth_lookup),
            },
            "trades": {
                "btcusdt" : tradesflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_trades_lookup),
                "btcusd" : tradesflow('okx', 'btc_usd', 'perpetual', level_size, lookups.okx_trades_lookup),
            },
            "oifunding" : {
                "btcusdt" : oiFundingflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_OI_lookup, lookups.okx_funding_lookup),
                "btcusd" : oiFundingflow('okx', 'btc_usd', 'perpetual', level_size, lookups.okx_OI_lookup, lookups.okx_funding_lookup),
            },
            "liquidations" : {
                "btc" : liquidationsflow('okx', 'btc_usdt', 'perpetual', level_size, lookups.okx_liquidations_lookup),
            },
            "indicators" : {
                "GTA" : indicatorflow('okx', 'btc_usdt', 'perpetual', "TTA", lookups.okx_GTA_lookup),
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
        self.perpetual_axis["indicators"]["GTA"].input_okx_gta(data)


    # Testing
    
    @classmethod
    def create_class(cls, level_size=20):
        return cls(level_size)

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


    def test_empty_dataframes(self):
        """
            My json files liquidaitons is empty because there is no data, better try with trades
        """

        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                if flow == "indicators":
                    if not bool( self.spot_axis[flow][instrument].data):
                        print(f"spot_axis, {flow}, {instrument}, is empty")


        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        if s.empty:
                            print(f"perpetual_axis, {flow}, {instrument}, is empty")
                    except:
                        print(f"perpetual_axis, {flow}, {instrument}, is NoneType")
                    
                if flow == "indicators":
                    if not bool( self.perpetual_axis[flow][instrument].data):
                        print(f"perpetual_axis, {flow}, {instrument}, is empty")


    def test_dataframes(self):
        # Observe snapshots for any discrepancy
        for flow in self.spot_axis.keys():
            for instrument in self.spot_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.spot_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.spot_axis[flow][instrument].snapshot_total
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.spot_axis[flow][instrument].data
                    if len(s) != 0:
                        print(s)
                time.sleep(5)
                clear_output(wait=True)

        for flow in self.perpetual_axis.keys():
            for instrument in self.perpetual_axis[flow].keys():
                if flow != "indicators":
                    if flow in ["books", "oifunding"]:
                        s = self.perpetual_axis[flow][instrument].snapshot
                    if flow in ["trades", "liquidations"]:
                        s = self.perpetual_axis[flow][instrument].snapshot_total
                    try:
                        display(s)
                    except:
                        pass
                if flow == "indicators":
                    s = self.perpetual_axis[flow][instrument].data
                    if not bool(s):
                        print(s)
                time.sleep(5)
                clear_output(wait=True)