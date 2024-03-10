import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from utilis import merge_suffixes, oiflow_merge_columns, synthesis_Trades_mergeDict, last_non_zero, is_valid_dataframe

class booksmerger():


    def __init__(self, instrument : str, insType : str, axis : dict):
        """
            axis : A dictionary that encapsulates a collection of books originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
            self.snapshotO - dictionary of the last books of this minute
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.snapshot = pd.DataFrame()
        self.suffixes = merge_suffixes(len(axis))
        self.snapshotO = dict()  # Omnified

    def retrive_data(self, key=None):
        """
            Arguments

            Single values:
                timestamp

            Heatmaps:
                books
        """
        if key != None:
            return self.snapshotO.get(key, None)
        else:
            return self.snapshotO

    def merge_snapshots(self):
        
        snapshots = [self.axis[ex].snapshot for ex in self.axis.keys()]
        snapshots = [df for df in snapshots if is_valid_dataframe(df)]
        
        for index, df in enumerate(snapshots):
            if index == 0:
                merged_df = pd.merge(snapshots[0], snapshots[1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
            if index == len(snapshots)-1:
                break
            if index != 0 and index != len(snapshots)-1:
                merged_df = pd.merge(merged_df, snapshots[index+1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
      
        common_columns_dic = {column.split("_")[0] : [] for column in merged_df.columns.tolist()}
        for column in merged_df.columns.tolist():
            common_columns_dic[column.split("_")[0]].append(column)
        
        sum = pd.DataFrame()
        for common_columns in common_columns_dic.keys():
            for index, column in enumerate(common_columns_dic[common_columns]):
                if index == 0 and "price" not in column:
                    sum[common_columns] = merged_df[column]
                if "price" not in column and index != 0:
                    sum[common_columns] = sum[common_columns] + merged_df[column]

        self.snapshot = sum.copy()
        sorted_columns = sorted(map(float, [c for c in self.snapshot.columns if "price" not in c]))
        self.snapshot = self.snapshot[map(str, sorted_columns)]

        self.snapshotO["books"] = dict(zip(self.snapshot.columns.tolist(), self.snapshot.iloc[-1].values))
        self.snapshotO["timestamp"] = (datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")


class tradesmerger():

    def __init__(self, instrument : str, insType : str, axis : dict):
        """
            axis : A dictionary that encapsulates a collection of trades originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
            self.snapshotO.... - dictionary of the statistics of the last trades over 60 seconds
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.snapshot_buys = pd.DataFrame()
        self.snapshot_sells = pd.DataFrame()
        self.snapshot_total = pd.DataFrame()
        self.suffixes = merge_suffixes(len(axis))
        self.snapshotO = dict() # Omnified

    def retrive_data(self, key= None):
        """
            Arguments

            Single values:
                timestamp, buyVolume, sellVolume, open, close
                low, high, volatility, numberBuyTrades, numberSellTrades

            Heatmaps:
                volumeProfile, buyVolumeProfile, sellVolumeProfile
            
            History of changes: (a list of trades in chronological order over 60 seconds)
                buyTrades, sellTrades
        """
        if key != None:
            return self.snapshotO.get(key, None)
        else:
            return self.snapshotO



    def merge_snapshots(self):

        snapshots_buys = [self.axis[ex].snapshot_buys for ex in self.axis.keys()]
        snapshots_sells = [self.axis[ex].snapshot_sells for ex in self.axis.keys()]
        snapshots_total = [self.axis[ex].snapshot_total for ex in self.axis.keys()]

        snapshots_buys = [df for df in snapshots_buys if is_valid_dataframe(df)]
        snapshots_sells = [df for df in snapshots_sells if is_valid_dataframe(df)]
        snapshots_total = [df for df in snapshots_total if is_valid_dataframe(df)]

        self.merge_snapshots_helper(snapshots_buys, "buys")
        self.merge_snapshots_helper(snapshots_sells, "sells")
        self.merge_snapshots_helper(snapshots_total, "total")

        self.snapshotO["timestamp"]  = (datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
        self.snapshotO["buyVolume"] = self.snapshot_buys.copy().drop(["price"], axis=1).sum().sum()
        self.snapshotO["sellVolume"] = self.snapshot_sells.copy().drop(["price"], axis=1).sum().sum()
        self.snapshotO["open"] = self.snapshot_total.iloc[0, 0]
        self.snapshotO["close"] = self.snapshot_total.iloc[-1, 0]
        self.snapshotO["low"] = self.snapshot_total["price"].min()
        self.snapshotO["high"] = self.snapshot_total["price"].max()
        self.snapshotO["volatility"] = self.snapshot_total["price"].std()
        self.snapshotO["volumeProfile"] = dict(zip(self.snapshot_total.copy().drop(["price"], axis=1).columns.tolist(), self.snapshot_total.copy().drop(["price"], axis=1).sum(axis=0)))
        self.snapshotO["buyVolumeProfile"] = dict(zip(self.snapshot_buys.copy().drop(["price"], axis=1).columns.tolist(), self.snapshot_buys.copy().drop(["price"], axis=1).sum(axis=0)))
        self.snapshotO["sellVolumeProfile"] = dict(zip(self.snapshot_sells.copy().drop(["price"], axis=1).columns.tolist(), self.snapshot_sells.copy().drop(["price"], axis=1).sum(axis=0)))
        self.snapshotO["numberBuyTrades"] = sum([self.axis[ex].numberBuyTrades for ex in self.axis.keys()])
        self.snapshotO["numberSellTrades"] = sum([self.axis[ex].numberSellTrades for ex in self.axis.keys()])
        self.snapshotO["buyTrades"] = synthesis_Trades_mergeDict([self.axis[ex].buyTrades for ex in self.axis.keys()])
        self.snapshotO["sellTrades"] = synthesis_Trades_mergeDict([self.axis[ex].sellTrades for ex in self.axis.keys()])

    
    def merge_snapshots_helper(self, snapshots, side):

        for index, df in enumerate(snapshots):
            if index == 0:
                merged_df = pd.merge(snapshots[0], snapshots[1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
            if index == len(snapshots)-1:
                break
            if index != 0 and index != len(snapshots)-1:
                merged_df = pd.merge(merged_df, snapshots[index+1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
        
        common_columns_dic = {column.split("_")[0] : [] for column in merged_df.columns.tolist()}
        for column in merged_df.columns.tolist():
            common_columns_dic[column.split("_")[0]].append(column)
        
        sum = pd.DataFrame()
        price = merged_df[common_columns_dic['price'][0]].copy()
        for common_columns in common_columns_dic.keys():
            for index, column in enumerate(common_columns_dic[common_columns]):
                if index == 0 and "price" not in column:
                    sum[common_columns] = merged_df[column]
                if "price" not in column and index != 0:
                    sum[common_columns] = sum[common_columns] + merged_df[column]
        sum["price"] = price

        if side == "buys":
            self.snapshot_buys = sum.copy()
            sorted_columns = sorted(map(float, [c for c in self.snapshot_buys.columns if "price" not in c]))
            self.snapshot_buys = self.snapshot_buys[map(str, ["price"] + sorted_columns)]
        if side == "sells":
            self.snapshot_sells = sum.copy()
            sorted_columns = sorted(map(float, [c for c in self.snapshot_sells.columns if "price" not in c]))
            self.snapshot_sells = self.snapshot_sells[map(str, ["price"] + sorted_columns)]
        if side == "total":
            self.snapshot_total = sum.copy()
            sorted_columns = sorted(map(float, [c for c in self.snapshot_total.columns if "price" not in c]))
            self.snapshot_total = self.snapshot_total[map(str, ["price"] + sorted_columns)]




class oiomnifier():

    """
        Returns aggregated summary over 60 seconds of funding, open interest and its statistics in self.snapshot 
    """

    def __init__(self, instrument : str, insType : str, axis : dict):
        """
            axis : A dictionary that encapsulates a collection of open interests and funding rates originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.snapshot = dict()
        self.suffixes = merge_suffixes(len(axis))

    def retrive_data(self, key):
        """
            Arguments

            Single values:
                timestamp, price, weighted_funding, total_oi, oi_increases
                oi_volatility, oi_change, 

            Heatmaps:
                oi_increases_std, oi_decreases, oi_decreases_std
                oi_turnover, oi_turnover_std, oi_total, oi_total_std
            
            History of changes: (a list of changes in chronological order over 60 seconds)
                ois
            Dicrionary of ois per instrument
                OIs_per_instrument
        """
        if key != None:
            return self.snapshot.get(key, None)
        else:
            return self.snapshot


    def merge_snapshots(self):
        snapshots = [self.axis[ex].snapshot for ex in self.axis.keys()]
        snapshots = [df for df in snapshots if is_valid_dataframe(df)]
        self.merge_snapshots_helper(snapshots)
        oi_instruments = {ex : self.axis[ex].snapshot.iloc[-1].loc["oi"] for ex in self.axis.keys() if is_valid_dataframe(self.axis[ex].snapshot)}
        fundings = {ex : self.axis[ex].snapshot.iloc[-1].loc["fundingRate"] for ex in self.axis.keys() if is_valid_dataframe(self.axis[ex].snapshot)}
        self.snapshot["OIs_per_instrument"] = oi_instruments
        self.snapshot["fundings_per_instrument"] = fundings

    def merge_snapshots_helper(self, snapshots):

        for index, df in enumerate(snapshots):
            if index == 0:
                merged_df = pd.merge(snapshots[0], snapshots[1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
            if index == len(snapshots)-1:
                break
            if index != 0 and index != len(snapshots)-1:
                merged_df = pd.merge(merged_df, snapshots[index+1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
              
        common_columns_dic = {column.split("_")[0] : [] for column in merged_df.columns.tolist()}
        for column in merged_df.columns.tolist():
            common_columns_dic[column.split("_")[0]].append(column)

        last_price = merged_df[common_columns_dic['price'][0]].iloc[-1]
        last_ois = merged_df[common_columns_dic["oi"]].iloc[-1].values
        last_fundings = merged_df[common_columns_dic["fundingRate"]].iloc[-1].values
        mask = ~pd.isna(last_fundings)
        weighted_funding = np.average(last_fundings[mask].astype(float), weights=last_ois[mask])
        total_oi = np.sum(last_ois)

        self.snapshot["price"] = last_price
        self.snapshot["weighted_funding"] = weighted_funding
        self.snapshot["total_oi"] = total_oi
        self.snapshot["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        columnstoremove = common_columns_dic["oi"] + common_columns_dic["fundingRate"] + common_columns_dic["price"]
        
        oi_increases = merged_df.copy().drop(columns=columnstoremove).clip(lower=0)
        oi_increases = oiflow_merge_columns(common_columns_dic, oi_increases)
        oi_increases_std = oi_increases.copy().std(axis=0)
        oi_increases_total = oi_increases.copy().sum(axis=0)

        oi_decreases = merged_df.copy().drop(columns=columnstoremove).clip(upper=0).abs()
        oi_decreases = oiflow_merge_columns(common_columns_dic, oi_decreases)
        oi_decreases_std = oi_decreases.copy().std(axis=0)
        oi_decreases_total = oi_decreases.copy().sum(axis=0)

        oi_turnover = merged_df.copy().drop(columns=columnstoremove).abs()
        oi_turnover = oiflow_merge_columns(common_columns_dic, oi_turnover)
        oi_turnover_std = oi_turnover.copy().std(axis=0)
        oi_turnover = oi_turnover.copy().sum(axis=0)
        
        oi_total = merged_df.copy().drop(columns=columnstoremove)
        oi_total_std = oiflow_merge_columns(common_columns_dic, oi_total).std(axis=0)
        oi_total = oiflow_merge_columns(common_columns_dic, oi_total).sum(axis=0)

        self.snapshot["oi_increases"] = dict(zip(oi_increases_total.index.tolist(), oi_increases_total.values.tolist()))
        self.snapshot["oi_increases_std"] = dict(zip(oi_increases_std.index.tolist(), oi_increases_std.values.tolist()))   # The speed of opening positions ??? idk what could it be useful for
        self.snapshot["oi_decreases"] = dict(zip(oi_decreases_total.index.tolist(), oi_decreases_total.values.tolist()))
        self.snapshot["oi_decreases_std"] = dict(zip(oi_decreases_std.index.tolist(), oi_decreases_std.values.tolist()))
        self.snapshot["oi_turnover"] = dict(zip(oi_turnover.index.tolist(), oi_turnover.values.tolist()))
        self.snapshot["oi_turnover_std"] = dict(zip(oi_turnover_std.index.tolist(), oi_turnover_std.values.tolist()))
        self.snapshot["oi_total"] = dict(zip(oi_total.index.tolist(), oi_total.values.tolist()))
        self.snapshot["oi_total_std"] = dict(zip(oi_total_std.index.tolist(), oi_total_std.values.tolist()))

        self.snapshot["oi_change"] = sum(self.snapshot["oi_total"].values())
        self.snapshot["oi_volatility"] = oiflow_merge_columns(common_columns_dic, merged_df.copy().drop(columns=columnstoremove)).stack().reset_index(drop=True).std(axis=0)

        nestedOis = merged_df.drop(columns=columnstoremove).values
        nestedOis = nestedOis[nestedOis != 0]
        self.snapshot["ois"] = nestedOis.tolist()


class lomnifier():
    """
        Returns aggregated summary over 60 seconds of liquidations ans some statistics in self.snapshot 
    """
    def __init__(self, instrument : str, insType : str, axis : dict):
        """
            axis : A dictionary that encapsulates a collection ofliquidations originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.snapshot = dict()
        self.suffixes = merge_suffixes(len(axis))

    def retrive_data(self, key : str):
        """
            Arguments

            Single values:
                timestamp, price, longsTotal, shortsTotal 

            Heatmaps:
                longs, shorts
            
            History of liquidations: (a list of liquidations in chronological order)
                longsLiquidations, shortsLiquidations
        """
        if key != None:
            return self.snapshot.get(key, None)
        else:
            return self.snapshot

    def merge_snapshots(self):

        longs = [self.axis[ex].longs for ex in self.axis.keys()]
        shorts = [self.axis[ex].shorts for ex in self.axis.keys()]

        longs = [df for df in longs if is_valid_dataframe(df)]
        shorts = [df for df in shorts if is_valid_dataframe(df)]
        
        self.merge_snapshots_helper(longs, "long")
        self.merge_snapshots_helper(shorts, "short")

        self.snapshot["longsLiquidations"] = synthesis_Trades_mergeDict([self.axis[ex].longsList for ex in self.axis.keys()])
        self.snapshot["shortsLiquidations"] = synthesis_Trades_mergeDict([self.axis[ex].shortsList for ex in self.axis.keys()])     

    def merge_snapshots_helper(self, snapshots, side):

        for index, df in enumerate(snapshots):
            if index == 0:
                merged_df = pd.merge(snapshots[0], snapshots[1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
            if index == len(snapshots)-1:
                break
            if index != 0 and index != len(snapshots)-1:
                merged_df = pd.merge(merged_df, snapshots[index+1], how='outer', left_index=True, right_index=True, suffixes=(self.suffixes[index], self.suffixes[index+1]))
              
        common_columns_dic = {column.split("_")[0] : [] for column in merged_df.columns.tolist()}
        for column in merged_df.columns.tolist():
            common_columns_dic[column.split("_")[0]].append(column)

        sum = pd.DataFrame()
        price = merged_df[common_columns_dic['price'][0]].copy()
        for common_columns in common_columns_dic.keys():
            for index, column in enumerate(common_columns_dic[common_columns]):
                if index == 0 and "price" not in column:
                    sum[common_columns] = merged_df[column]
                if "price" not in column and index != 0:
                    sum[common_columns] = sum[common_columns] + merged_df[column]
        sum["price"] = price

        self.snapshot["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        self.snapshot["price"] =last_non_zero(sum["price"].values)

        if side == "long":
            total_longs = sum.copy().drop(["price"], axis=1).sum().sum()
            total_longs_heatmap = sum.copy().drop(["price"], axis=1).sum(axis=0)
            self.snapshot["longsTotal"] = total_longs
            self.snapshot["longs"] = dict(zip(total_longs_heatmap.index.tolist(), total_longs_heatmap.values.tolist()))
        if side == "short":
            total_longs = sum.copy().drop(["price"], axis=1).sum().sum()
            total_longs_heatmap = sum.copy().drop(["price"], axis=1).sum(axis=0)
            self.snapshot["shortsTotal"] = total_longs
            self.snapshot["shorts"] = dict(zip(total_longs_heatmap.index.tolist(), total_longs_heatmap.values.tolist()))


class booksadjustments():
    """
        Gathers statistical information on canceled limit and reinforced limit orders order books in the form of a heatmap variance over 60 second history
    """
    def __init__ (self, instrument : str, insType : str, instance_books : booksmerger, instance_trades : tradesmerger):
        """
            snapshot_voids : A dataset with canceled books over time
            snapshot_reinforce : A dataset with reinforced books over time
            data : dictionary of the last data 
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = {
            "books" : instance_books,
            "trades" : instance_trades,
        }
        self.snapshot_voids = pd.DataFrame()
        self.snapshot_reinforce = pd.DataFrame()
        self.data = dict()

    def retrive_data(self, key : str = None):
        """
            Arguments

            Single values:
                timestamp, price, totalVoids, totalReinforces (total voids and total reinforced orders over all levels at this timestamp)
                totalVoidsStd, totalReinforcesStd
            Heatmaps:
                voids, reinforces, voidsStd, reinforcesStd
        """
        if key != None:
            return self.data.get(key, None)
        else:
            return self.data


    def get_adjusted_orders(self):

        
        df_books = self.axis['books'].snapshot.copy()
        df_trades = self.axis['trades'].snapshot_total.copy()
        merged_df = pd.merge(df_books, df_trades, how='outer', left_index=True, right_index=True, suffixes=('_books', '_trades'))
        common_columns_trades = df_books.columns.intersection(df_trades.columns).tolist()
        uncommon_columns =  [col for col in df_books.columns.tolist() if col not in df_trades.columns.tolist()]

        snapshot = pd.DataFrame(dtype="float64")
        
        for column in common_columns_trades:
            if "price" not in column:
                snapshot[column] = merged_df[column + '_books'].sub(merged_df[column + '_trades'], fill_value=0)
        for column in uncommon_columns:
            if "price" not in column:
                snapshot[column] = df_books[column]
        sorted_columns = sorted(map(float, [c for c in snapshot.columns if "price" not in c]))
        snapshot = snapshot[map(str, sorted_columns)]
        for column in snapshot.columns.tolist():
            snapshot[column] = snapshot[column].shift(-1) - snapshot[column]
        snapshot = snapshot[:-1]

        voids = snapshot.copy().applymap(lambda x: abs(x) if x < 0 else 0)
        reinforces = snapshot.copy().applymap(lambda x: x if x > 0 else 0)

        self.data["price"] = df_trades["price"].iloc[-1]
        self.data["voids"] = dict(zip(voids.sum(axis=0).index.tolist(), voids.sum(axis=0).values.tolist()))
        self.data["reinforces"] = dict(zip(reinforces.sum(axis=0).index.tolist(), reinforces.sum(axis=0).values.tolist()))
        self.data["voidsStd"] = dict(zip(voids.std(axis=0).index.tolist(), voids.std(axis=0).values.tolist()))
        self.data["reinforcesStd"] = dict(zip(reinforces.std(axis=0).index.tolist(), reinforces.std(axis=0).values.tolist()))
        self.data["totalVoids"] = voids.sum().sum()
        self.data["totalReinforces"] = reinforces.sum().sum()
        self.data["totalVoidsStd"] = voids.sum().std()
        self.data["totalReinforcesStd"] = reinforces.sum().std()
        self.data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        for column in voids.columns:
            non_zero_timestamps = np.where(voids[column] != 0)[0]
            meddian_void_duration = np.median(np.diff(non_zero_timestamps))
            if meddian_void_duration != np.nan:
                if self.data.get("voidsDuration") == None:
                    self.data["voidsDuration"] = {}
                if self.data.get("voidsDurationStd") == None:
                    self.data["voidsDurationStd"] = {}
                self.data["voidsDuration"][column] = meddian_void_duration
                self.data["voidsDurationStd"][column] = np.std(non_zero_timestamps)

        for column in reinforces.columns:
            non_zero_timestamps = np.where(reinforces[column] != 0)[0]
            meddian_void_duration = np.median(np.diff(non_zero_timestamps))
            if meddian_void_duration != np.nan:
                if self.data.get("reinforcesDuration") == None:
                    self.data["reinforcesDuration"] = {}
                if self.data.get("reinforcesDurationStd") == None:
                    self.data["reinforcesDurationStd"] = {}
                self.data["reinforcesDuration"][column] = meddian_void_duration
                self.data["reinforcesDurationStd"][column] = np.std(non_zero_timestamps)



class OOImerger():
    """
        Aggregates Option data from different exchanges
    """
    def __init__(self, instrument : str, insType : str, expiry_windows : np.array, pranges : np.array, axis : dict):
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.expiry_windows = expiry_windows
        self.pranges = pranges
        self.data = {
            "puts" : {},
            "calls" : {}
        }

    def retrive_data(self, side=None, key_1=None, key_2=None):
        """
            - The function behavior depends on the values of expiry_windows and pranges:    

            side : puts, calls
            key_1 : expiry_windows 
                if expiry_windows = [0.0, 1.0, 3.0, 7.0], then legit keys are "0", "0_1" , "1_3" ... "7" all in string
            key_2 : pranges
               if pranges==[0.0, 1.0, 2.0, 5.0, 10.0],
               keys will be -5.0, -2.0, -1.0, 0.0, 1.0, 2.0, 5.0, 10.0  in str
        """
        if side != None and  key_1 != None and key_2 != None:
            return self.data[side][key_1][key_2]
        if side != None and  key_1 != None and key_2 == None:
            return self.data[side][key_1]      
        if side != None and  key_1 == None and key_2 == None:
            return self.data[side]
        if side == None and  key_1 == None and key_2 == None:
            return self.data
    
    def mergeoi(self):
        for exchange in self.axis .keys():
            for calls, puts in zip(self.axis [exchange].df_call.keys(), self.axis [exchange].df_put.keys()):
                try:
                    values_call = self.axis [exchange].df_call[calls].values.tolist()[0]
                    keys_call = self.axis [exchange].df_call[calls].columns.tolist()

                    values_put = self.axis [exchange].df_put[calls].values.tolist()[0]
                    keys_put = self.axis [exchange].df_put[calls].columns.tolist()

                    if calls not in self.data["calls"]:
                        self.data["calls"][calls] = {}        
                    for key, value in zip(keys_call, values_call):
                        if key not in self.data["calls"][calls]:
                            self.data["calls"][calls][key] = 0
                        self.data["calls"][calls][key] += value

                    if puts not in self.data["puts"]:
                        self.data["puts"][puts] = {}        
                    for key, value in zip(keys_put, values_put):
                        if key not in self.data["puts"][puts]:
                            self.data["puts"][puts][key] = 0
                        self.data["puts"][puts][key] += value 

                    self.data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                except:
                    continue     



class indomnifier():

    """
        Merges the same indicator from different exchanges weighted by total open interest
    """

    def __init__ (self, instrument : str, insType : str, indType : str, axis_ratio : dict, axis_oi : oiomnifier):
        """
            axis_ratio : the dictionary of classes indicatorsflow
        """
        self.instrument = instrument
        self.insType = insType
        self.indType = indType
        self.axis_ratio = axis_ratio
        self.axis_oi = axis_oi.axis
        self.data = dict()
        self.open_interest = dict()
        self.ratios = dict()

    def retrive_data(self, key=None):
        """
            timestamp, ratio
            is key == None, returns the whole dictionary
        """
        if key != None:
            return self.data.get(key)
        else:
            return self.data
        
    def merge_ratios(self):
        for instrument in self.axis_ratio:
            self.input_ratio( instrument, self.axis_ratio[instrument].retrive_data("ratio"))
        for instrument in self.axis_oi:
            self.input_oi(instrument, self.axis_oi[instrument].current_oi)
        keys_to_remove = ['_GTA', '_TTA', '_TTP']
        self.ratios = {key.replace(term, ''): value for key, value in self.ratios.items() for term in keys_to_remove if term in key}
        self.merge()
        

    def input_oi(self, instrument, data):
        """
            Inputs open interest by instrument 
        """
        self.open_interest[instrument] = data

    def input_ratio(self, instrument, data):
        """
            Inputs ratios by instrument 
        """
        self.ratios[instrument] = data


    def merge(self):
        """
            merges gta, gtp, tta, ttp's
            since okx provides global btc long shorts ration, the code has some slight changes
        """

        intersection = {key: (self.open_interest[key], self.ratios[key]) for key in self.open_interest.keys() & self.ratios.keys()}
        okx_ratio_keys = {key: value for key, value in self.ratios.items() if 'okx' in key}
        okx_oi_keys = {key: value for key, value in self.open_interest.items() if 'okx' in key}
        try:
            intersection[list(okx_ratio_keys.keys())[0]] = (sum(list(okx_oi_keys.values())), list(okx_ratio_keys.values())[0])
            ois = [x[0] for x in list(intersection.values())]
            ratios = [x[1] for x in list(intersection.values())]
            weighted_ratio = np.average(ratios, weights=ois)
            self.data["ratio"] = weighted_ratio
            self.data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        except:
            ois = [x[0] for x in list(intersection.values())]
            ratios = [x[1] for x in list(intersection.values())]
            weighted_ratio = np.average(ratios, weights=ois)
            self.data["ratio"] = weighted_ratio
            self.data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M")      

    