import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utilis import merge_suffixes, oiflow_merge_columns, synthesis_Trades_mergeDict

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

    def retrive_data(self, key):
        """
            Single values:
                timestamp

            Heatmaps:
                books
        """
        return self.snapshotO.get(key, None)

    def merge_snapshots(self):
        
        snapshots = [self.axis[ex].snapshot for ex in self.axis.keys()]
        
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

    def retrive_data(self, key):
        """
            Single values:
                timestamp, buyVolume, sellVolume, open, close
                low, high, volatility, numberBuyTrades, numberSellTrades

            Heatmaps:
                volumeProfile, buyVolumeProfile, sellVolumeProfile
            
            History of changes: (a list of trades in chronological order over 60 seconds)
                buyTrades, sellTrades
        """
        return self.snapshotO.get(key, None)


    def merge_snapshots(self):

        snapshots_buys = [self.axis[ex].snapshot_buys for ex in self.axis.keys()]
        snapshots_sells = [self.axis[ex].snapshot_sells for ex in self.axis.keys()]
        snapshots_total = [self.axis[ex].snapshot_total for ex in self.axis.keys()]

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
            Single values:
                timestamp, price, weighted_funding, total_oi, oi_increases
                oi_volatility, oi_change, 

            Heatmaps:
                oi_increases_std, oi_decreases, oi_decreases_std
                oi_turnover, oi_turnover_std, oi_total, oi_total_std
            
            History of changes: (a list of changes in chronological order over 60 seconds)
                ois
        """
        return self.snapshot.get(key, None)

    def merge_snapshots(self):
        snapshots = [self.axis[ex].snapshot for ex in self.axis.keys()]
        self.merge_snapshots_helper(snapshots)

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
        weighted_funding = np.average(last_fundings, weights=last_ois)
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

    def retrive_data(self, key):
        """
            Single values:
                timestamp, price, longsTotal, shortsTotal 

            Heatmaps:
                longs, shorts
            
            History of liquidations: (a list of liquidations in chronological order)
                longsLiquidations, shortsLiquidations
        """
        return self.snapshot.get(key, None)

    def merge_snapshots(self):

        longs = [self.axis[ex].longs for ex in self.axis.keys()]
        shorts = [self.axis[ex].shorts for ex in self.axis.keys()]
        
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
        self.snapshot["price"] =sum["price"].iloc[-1]

        if side == "long":
            total_longs = sum.copy().drop(["price"]).sum().sum()
            total_longs_heatmap = sum.copy().drop(["price"]).sum(axis=0)
            self.snapshot["longsTotal"] = total_longs
            self.snapshot["longs"] = dict(zip(total_longs_heatmap.index.tolist(), total_longs_heatmap.values.tolist()))
        if side == "short":
            total_longs = sum.copy().drop(["price"]).sum().sum()
            total_longs_heatmap = sum.copy().drop(["price"]).sum(axis=0)
            self.snapshot["shortsTotal"] = total_longs
            self.snapshot["shorts"] = dict(zip(total_longs_heatmap.index.tolist(), total_longs_heatmap.values.tolist()))


# class omnify(mergemaster):
#     """
#        Universal 1 minute snapshot merger summarizer, Books, OI, Liquidations, Trades, Voids
#     """
#     def __init__(self, *args, **kwargs):
#         """
#             books: 
#                 snapshot_lastbooks - 1m price close, open, high, low, price standart deviation, last books of each levels
#                 snapshot_booksvar - 1m price close, total standart deviation of books, standart deviation of books over 1 minute at each price level
#             trades:
#                 snapshot_trades - 1m price close, 1 min volume traded, trades distribution over each price level
#                 snapshot_tradesstd - 1m price close, 1 min total standart deviation of books, standart deviation distribution over each price level
#             oi : 
#                 snapshot_oi - 1m price close, total increase/decrease of open interest, distribution of open interest increase decrease over each price level
#                 snapshot_oistd -  1m price close, total standart deviation of open interest, standart deviation of open interest over each price level
#             voids : 
#                 snapshot_voids - 1m price close, 1 min volume of closed books at over every price level
#                 snapshot_voidsstd -  1m price close, 1 min volume std of closed books at over every price level
#             liquidations:
#                 snapshot_liquidations - 1m price close, total liquidations over 1 minute, liquidations at each price level
#         """
#         super().__init__(*args, **kwargs)
#         if flowType == 'books':
#             self.snapshot_lastbooks = pd.DataFrame()
#             self.snapshot_booksvar = pd.DataFrame()
#         if flowType == 'trades':
#             self.snapshot_trades = pd.DataFrame()
#             self.snapshot_tradesvar = pd.DataFrame()
#         if flowType == 'oi':
#             self.snapshot_oi = pd.DataFrame()
#             self.snapshot_oivar = pd.DataFrame()
#         if flowType == 'liquidations':
#             self.snapshot_liquidations = pd.DataFrame()


#     def omnify_snapshots(self):
        
#         super().merge_snapshots()

#         if self.flowType == 'books':
            
#             # Last books
#             self.snapshot_lastbooks = self.snapshot.drop(columns=['price']).iloc[-1].T
#             openn = self.napshot['price'].values[0]
#             low = self.snapshot['price'].values.min()
#             high = self.snapshot['price'].values.max()
#             close = self.snapshot['price'].values[-1]
#             price_var = self.snapshot['price'].std()
#             for index, col, value in enumerate(zip(['open', 'low', 'high', 'close', 'price_var'], [openn, low, high, close, price_var])):
#                 self.snapshot_lastbooks.insert(loc=index, column=col, value=[value])
            
#             # Books var
#             self.snapshot_booksvar = self.snapshot.var().T
#             renamed_columns = ["".join([col, "_var"]) for col in self.snapshot_booksvar.columns.tolist()]
#             self.snapshot_booksvar = self.snapshot_booksvar.rename(columns=dict(zip(self.snapshot_booksvar.columns, renamed_columns)))
#             total_var = self.snapshot.std().sum()
#             for index, col, value in enumerate(zip(['close', 'books_var'], [close, total_var])):
#                 self.snapshot_booksvar.insert(loc=index, column=col, value=[value])            


#         if self.flowType == 'trades':

#             # trades
#             price = self.snapshot.copy()['price'].values[-1]
#             df = self.snapshot.copy().drop(columns=['price'])
#             # Drop columns with only 0s
#             df = df.loc[(df != 0).any(axis=1)]
#             total_volume = df.sum().sum()
#             self.snapshot_trades = df.sum().T
#             for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_volume])):
#                 self.snapshot_trades.insert(loc=index, column=col, value=[value])

#             # trades variance. Higher variance indicate the presence of block trades
#             df = self.snapshot.copy().drop(columns=['price'])
#             self.snapshot_tradesvar = df.loc[(df != 0).any(axis=1)].var().T
#             total_variance = self.snapshot_trades.sum()
#             for index, col, value in enumerate(zip(['price', 'volume_variance'], [price, total_variance])):
#                 self.snapshot_tradesvar.insert(loc=index, column=col, value=[value])

#         if self.flowType == 'oi':

#             # oi increase/decrease
#             price = self.snapshot.copy()['price'].values[-1]
#             df = self.snapshot.copy().drop(columns=['price'])
#             df = df.loc[(df != 0).any(axis=1)]
#             total_oi = df.sum().sum()
#             self.snapshot_oi = df.sum().T
#             for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
#                 self.snapshot_oi.insert(loc=index, column=col, value=[value])

#             # on increase/decrease variance
#             self.snapshot_oivar  = self.snapshot.copy().drop(columns=['price'])
#             self.snapshot_oivar  = self.snapshot_oivar.loc[(df != 0).any(axis=1)].var().T
#             total_oi = self.snapshot_oivar.sum()
#             for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
#                 self.snapshot_oivar.insert(loc=index, column=col, value=[value])


#         if self.flowType == 'liquidations':
#             price = self.snapshot.copy()['price'].values[-1]
#             self.snapshot_liquidations  = self.snapshot.copy().drop(columns=['price'])
#             total_liquidations = self.snapshot_liquidations.sum().sum()
#             self.snapshot_liquidations = self.snapshot_liquidations.sum().T
#             for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_liquidations])):
#                 self.snapshot_oivar.insert(loc=index, column=col, value=[value])





class OOImerger():
    """
        Aggregates Option data from different exchanges
    """
    def __init__(self, processes: list, number_rows=741):
        """
            number_rows = The length of the dataset to keep
        """
        self.processes = processes
        self.snapshot_columns = ["price"]+[f"{vvv}_{v}_{vv}" for vvv in ["call", "put"] for v in self.processes[0].df_call.keys() for vv in list(self.processes[0].df_call[next(iter(self.processes[0].df_call))].columns)]
        self.snapshot = pd.DataFrame(index=pd.to_datetime([], utc=True), columns=[self.snapshot_columns]).rename_axis('timestamp')
        self.number_rows = number_rows

    def input_data(self, raw_data, index_price):

        dic_df_call = { key : pd.DataFrame() for key in self.processes[0].df_call.keys()}
        dic_df_put = { key : pd.DataFrame() for key in self.processes[0].df_put.keys()}
        
        for c, data in zip(self.processes, raw_data):
            c.input_oi(data, index_price)

        for c in self.processes:
            for key in c.df_put.keys():
                dic_df_call[key] = pd.concat([dic_df_call[key], c.df_call[key]], ignore_index=True)
                dic_df_put[key] = pd.concat([dic_df_call[key], c.df_put[key]], ignore_index=True)

        for frame in dic_df_call.keys():
            dic_df_call[frame] = dic_df_call[frame].sum().to_frame().T
            dic_df_put[frame] = dic_df_put[frame].sum().to_frame().T

        self.snapshot.loc[pd.to_datetime(time.time(), unit='s', utc=True)]  = np.concatenate((np.array([float(index_price)]), pd.concat([dic_df_put[x] for x in dic_df_call] + [dic_df_put[x] for x in dic_df_put], axis=1, ignore_index=True).values[0]))

        if len(self.snapshot) > self.number_rows:
            self.snapshot.iloc[1:]


