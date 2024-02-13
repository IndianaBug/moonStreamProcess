import numpy as np 
import pandas as pd 
import datetime
import json
from utilis import *

class booksflow():
    """
        Important notes: 
            Maintain consistency in current timestamp across all flows
            If the book is above book_ceil_thresh from the current price, it will be omited for the next reasons:
                - Computational efficiency
                - Who books so high if they want to trade now? Challange this statemant ...
            Aggregation explanation:  If the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40, and so forth.
    """
    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, lookup : callable, book_ceil_thresh=5):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon (measured in unites of the quote to base pair)
            book_ceil_thresh : % ceiling of price levels to ommit, default 5%
            lookup : a function to get formated timestamp,  bids and asks from a dictionary
        """
        # Identification
        self.exchange = exchange
        self.symbol = symbol
        self.insType = insType
        self.lookup = lookup
        self.level_size = float(level_size)
        self.book_ceil_thresh = book_ceil_thresh
        self.df = pd.DataFrame()
        self.B = {"timestamp" : None, "bids" : {}, "asks" : {}}
        self.snapshot = None
        self.previous_second = -1
        self.current_second = 0
        self.price = 0

    
    def update_books(self, books):
        
        try:
            bids, timestamp = self.lookup(books, "bids")
            asks, timestamp = self.lookup(books, "asks")
        except:
            return

        self.B['timestamp'] = timestamp
        try:
            self.price = (max(self.B['bids'].keys()) + min(self.B['asks'].keys())) / 2
        except:
            self.price = (bids[0][0] + asks[0][0]) / 2
         
        self.update_books_helper(bids, "bids")
        self.update_books_helper(asks, "asks")
        
        self.current_second = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').second

        if self.current_second > self.previous_second:
            self.dfs_input_books()
            self.previous_second = self.current_second
        if self.previous_second > self.current_second:
            for col in self.df.columns:
                self.df[col] = self.df[col].replace(0, pd.NA).ffill()
                self.df[col] = self.df[col].replace(0, pd.NA).bfill()
            self.snapshot = self.df.copy()
            self.df = pd.DataFrame()
            self.previous_second = self.current_second
            # Delete unnecessary data
            booksflow_datatrim(self.price, self.B, 'bids', self.book_ceil_thresh)
            booksflow_datatrim(self.price, self.B, 'asks', self.book_ceil_thresh)
            # Start everything all over again
            self.dfs_input_books()

    def update_books_helper(self, books, side):
        """
          side: bids, asks
        """
        # Omit books above 5% from the current price
        for book in books:
            p = book[0]
            a = book[1]
            if abs(booksflow_compute_percent_variation(p, self.price)) > self.book_ceil_thresh:
                continue
            if a == 0:
                try:
                    del self.B[side][book[0]]
                except:
                    pass
            else:
                self.B[side][p] = a

    def dfs_input_books(self):
        """
            Inputs bids and asks into dfs
        """

        prices = np.array(list(map(float, self.B['bids'].keys())) + list(map(float, self.B['asks'].keys())), dtype=np.float16)
        amounts = np.array(list(map(float, self.B['bids'].values())) + list(map(float, self.B['asks'].values())), dtype=np.float16)
        levels = [booksflow_find_level(lev, self.level_size) for lev in  prices]
        unique_levels, inverse_indices = np.unique(levels, return_inverse=True)
        group_sums = np.bincount(inverse_indices, weights=amounts)
        columns = [str(col) for col in unique_levels]

        if self.df.empty:
            self.df = pd.DataFrame(0, index=list(range(60)), columns = columns, dtype='float64')
            self.df.loc[self.current_second] = group_sums
            sorted_columns = sorted(map(float, self.df.columns))
            self.df = self.df[map(str, sorted_columns)]
        else:
            old_levels = np.array(self.df.columns, dtype=np.float64)
            new_levels = np.setdiff1d(np.array(columns, dtype=np.float64), old_levels)
            for l in new_levels:
                self.df[str(l)] = 0
                self.df[str(l)]= self.df[str(l)].astype("float64")
            sums = booksflow_manipulate_arrays(old_levels, np.array(columns, dtype=np.float64), group_sums)
            self.df.loc[self.current_second] = sums
            sorted_columns = sorted(map(float, self.df.columns))
            self.df = self.df[map(str, sorted_columns)] 




class tradesflow():
    """
        Important notes: 
            Maintain consistency in the current timestamp across all flows
            Aggregation explanation:  If the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40, and so forth.
    """

    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, lookup : callable):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon (measured in unites of the quote to base pair)
            lookup : function to extract details from the response
        """
        self.exchange = exchange
        self.symbol = symbol
        self.insType = insType
        self.level_size = float(level_size)
        self.lookup = lookup
        self.buys = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype='float64')
        self.sells = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype='float64')
        self.snapshot_buys = None
        self.snapshot_sells = None
        self.snapshot_total = None
        self.previous_second = -1
        self.current_second = 0

    def input_trades(self, data) :
        try:
            for trade in self.lookup(data):
                try:
                    side, price, amount, timestamp = trade
                    self.dfs_input_trade(side, price, amount, timestamp)
                except:
                    continue
        except:
            return


    def dfs_input_trade(self, side, price, amount, timestamp):

        self.current_second = int(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').second)

        if self.previous_second > self.current_second:

            self.snapshot_buys = self.buys.copy()
            self.snapshot_buys.fillna(0, inplace = True)
            self.snapshot_sells = self.sells.copy()
            self.snapshot_sells.fillna(0, inplace = True)
            
            self.snapshot_buys['price'] = self.snapshot_buys['price'].replace(0, pd.NA).ffill()
            self.snapshot_buys['price'] = self.snapshot_buys['price'].replace(0, pd.NA).bfill()
            self.snapshot_sells['price'] = self.snapshot_sells['price'].replace(0, pd.NA).ffill()
            self.snapshot_sells['price'] = self.snapshot_sells['price'].replace(0, pd.NA).bfill()

            merged_df = pd.merge(self.snapshot_buys.copy(), self.snapshot_sells.copy(), left_index=True, right_index=True, how='outer', suffixes=('_buys', '_sells')).fillna(0)

            common_columns_dic = {column.split("_")[0] : [] for column in merged_df.columns.tolist()}
            for column in merged_df.columns.tolist():
                common_columns_dic[column.split("_")[0]].append(column)

            self.snapshot_total = pd.DataFrame()
            for common_columns in common_columns_dic.keys():
                for index, column in enumerate(common_columns_dic[common_columns]):
                    if index == 0 and "price" not in column:
                        self.snapshot_total[common_columns] = merged_df[column]
                    if "price" not in column:
                        self.snapshot_total[common_columns] = self.snapshot_total[common_columns] + merged_df[column]
            self.snapshot_total.insert(0, 'price', self.snapshot_buys['price'])

    
            self.buys = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype='float64')
            self.sells = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype='float64')

        self.previous_second = self.current_second

        if side == 'buy':
            self.buys.loc[self.current_second, 'price'] = price
            level = booksflow_find_level(price, self.level_size)
            current_columns = (map(float, [x for x in self.buys.columns.tolist() if x != "price"]))
            if level not in current_columns:
                self.buys[str(level)] = 0
                self.buys[str(level)] = self.buys[str(level)].astype("float64")
                self.buys.loc[self.current_second, str(level)] += amount
            else:
                self.buys.loc[self.current_second, str(level)] += amount

        
        if side == 'sell':
            self.sells.loc[self.current_second, 'price'] = price
            level = booksflow_find_level(price, self.level_size)
            current_columns = (map(float, [x for x in self.sells.columns.tolist() if x != "price"]))
            if level not in current_columns:
                self.sells[str(level)] = 0
                self.sells[str(level)] = self.sells[str(level)].astype("float64")
                self.sells.loc[self.current_second, str(level)] += amount
            else:
                self.sells.loc[self.current_second, str(level)] += amount






class oiFundingflow():
    """
        Important notes: 
            Maintain consistency in the current timestamp across all flows
            Aggregation explanation:  If the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40, and so forth.
    """

    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, lookup_oi : callable, lookup_funding : callable = None):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon (measured in unites of the quote to base pair
            lookup : a function that returns formated oi with timestamp from response
            Some apis fetch both funding and oi altogether, most doesn't. 
            If api does, lookup_oi should look for both funding and oi 
        """
        self.exchange = exchange
        self.symbol = symbol
        self.insType = insType
        self.level_size = float(level_size)
        self.lookup_oi = lookup_oi
        self.lookup_funding = lookup_funding
        self.raw_data = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price', 'fundingRate', "oi"]), dtype='float64')
        self.snapshot = None
        self.previous_second = -1
        self.current_second = 0
        self.previous_oi = None
        self.fundingRate = 0

    
    def input_oi_funding(self, oifundingdata):
        try:
            funding, openInterestValue, price, timestamp = self.lookup_oi(oifundingdata)
            self.fundingRate = funding
            self.dfs_input(openInterestValue, price, timestamp)
        except:
            return

    def input_funding(self, fundingdata):
        try:
            funding, price, timestamp = self.lookup_funding(fundingdata)
            self.fundingRate = funding
        except:
            return
    
    def input_oi(self, oidata):
        try:
            oi, price, timestamp = self.lookup_oi(oidata)
            self.dfs_input(oi, price, timestamp)
        except:
            return
    

    def dfs_input(self, oi, price, timestamp):
        
        self.current_second = int(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').second)

        if self.previous_oi == None:
            self.previous_oi = oi

        amount = oi - self.previous_oi

        if self.previous_second > self.current_second:
            self.snapshot = self.raw_data.copy()
            self.snapshot.fillna(0, inplace = True)
            for col in ['price', 'fundingRate', "oi"]:
                self.snapshot[col] = self.snapshot[col].replace(0, pd.NA).ffill()
                self.snapshot[col] = self.snapshot[col].replace(0, pd.NA).bfill()
            self.raw_data = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price', 'fundingRate', "oi"]), dtype='float64')
        self.previous_second = self.current_second

        self.raw_data.loc[self.current_second, 'price'] = price
        level = booksflow_find_level(price, self.level_size)
        current_columns = (map(float, [x for x in self.raw_data.columns.tolist() if x not in ['price', 'fundingRate', "oi"]]))
        if level not in current_columns:
            self.raw_data[str(level)] = 0
            self.raw_data[str(level)] = self.raw_data[str(level)].astype("float64")
            self.raw_data.loc[self.current_second, str(level)] = amount
        else:
            self.raw_data.loc[self.current_second, str(level)] = amount
        self.raw_data.loc[self.current_second, "oi"] = oi
        self.raw_data.loc[self.current_second, "fundingRate"] = self.fundingRate

        self.previous_oi = oi




class liquidationsflow():
    """
        Important notes: 
            Maintain consistency in the current timestamp across all flows
            Aggregation explanation:  If the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40, and so forth.
    """

    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, lookup : callable):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon (measured in unites of the quote to base pair)
        """
        self.exchange = exchange
        self.symbol = symbol
        self.insType = insType
        self.level_size = float(level_size)
        self.lookup = lookup
        self.longs = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype="float64")
        self.shorts = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype="float64")
        self.snapshot_longs = None
        self.snapshot_shorts = None
        self.snapshot_total = None
        self.previous_second = -1
        self.current_second = 0


    def input_liquidations(self, data):
        try:
            for liq in self.lookup(data):
                side = liq[0]
                price = liq[1]
                amount = liq[2]
                timestamp = liq[3]
                self.dfs_input_liquidations(side, price, amount, timestamp)
        except:
            pass

    def dfs_input_liquidations(self, side, price, amount, timestamp):

        self.current_second = int(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').second)

        if self.previous_second > self.current_second:

            self.snapshot_longs = self.longs.copy()
            self.snapshot_shorts = self.longs.copy()
            self.snapshot_total = self.longs.copy() + self.shorts.copy()
            
            for col in ['price']:
                self.longs[col] = self.longs[col].ffill()
                self.longs[col] = self.longs[col].bfill()
                self.shorts[col] = self.shorts[col].ffill()
                self.shorts[col] = self.shorts[col].bfill()

            self.longs = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype="float64")
            self.shorts = pd.DataFrame(0, index=list(range(0, 60, 1)) , columns=np.array(['price']), dtype="float64")
        
        self.previous_second = self.current_second

        if side == "buy":
            self.longs.loc[self.current_second, 'price'] = price
            level = booksflow_find_level(price, self.level_size)
            current_columns = (map(float, [x for x in self.longs.columns.tolist() if x != "price"]))
            if level not in current_columns:
                self.longs[str(level)] = 0
                self.longs[str(level)] = self.longs[str(level)].astype("float64")
                self.longs.loc[self.current_second, str(level)] += amount
            else:
                self.longs.loc[self.current_second, str(level)] += amount

        if side == "sell":
            self.shorts.loc[self.current_second, 'price'] = price
            level = booksflow_find_level(price, self.level_size)
            current_columns = (map(float, [x for x in self.shorts.columns.tolist() if x != "price"]))
            if level not in current_columns:
                self.shorts[str(level)] = 0
                self.shorts[str(level)] = self.shorts[str(level)].astype("float64")
                self.shorts.loc[self.current_second, str(level)] += amount
            else:
                self.shorts.loc[self.current_second, str(level)] += amount



class oiflowOption():

    """ 
        example:
        pranges = np.array([0.0, 1.0, 2.0, 5.0, 10.0])  : percentage ranges of strikes from current price
        expiry_windows = np.array([0.0, 1.0, 3.0, 7.0])  : expiration window ranges

    """

    def __init__ (self, exchange : str, instrument : str, insType : str, pranges : np.array,  expiry_windows : np.array, lookup : callable):
        self.exchange = exchange
        self.instrument = instrument
        self.lookup = lookup
        self.insType = insType
        self.pranges = pranges
        self.expiry_windows = expiry_windows
        self.df_call = dict()
        self.df_put = dict()

    def input_oi(self, data):
        try:
            self.input_oi_helper(data, "C")
            self.input_oi_helper(data, "P")
        except:
            return

    def input_oi_helper(self, data : dict, side : str):
        strikes, countdowns, oi, price, timestamp = self.lookup(data, side)
        timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').replace(minute=0, second=0)
        options_data = {"strikes" : strikes, "countdown" :countdowns, "oi" : oi}
        df = pd.DataFrame(options_data).groupby(['countdown', 'strikes']).sum().reset_index()
        df = df[(df != 0).all(axis=1)]
        dictDataFrame = build_option_dataframes(self.expiry_windows, self.pranges)
        helper = oiflowOption_dictionary_helper(dictDataFrame, countdowns)
        fullPranges = oiflowOption_getranges(self.pranges)
        for dfid in helper.keys():
            empty_df = pd.DataFrame()
            for countdown in helper[dfid]:
                d = df[df['countdown'] == countdown ].drop(columns=['countdown'])
                d['pcd'] = df['strikes'].apply(lambda x : getpcd(price, x))
                d['range'] = d['pcd'].apply(lambda x: oiflowOption_choose_range(fullPranges, x))
                d = d.groupby(['range']).sum().reset_index().drop(columns=["strikes", "pcd"]).set_index('range')
                missing_values = np.setdiff1d(fullPranges, d.index.values)
                new_rows = pd.DataFrame({'oi': 0}, index=missing_values)
                combined_df = pd.concat([d, new_rows])
                combined_df = combined_df.transpose() 
                combined_df['timestamp'] = pd.to_datetime(timestamp)
                combined_df.set_index('timestamp', inplace=True)
                combined_df = combined_df.sort_index(axis=1)
                empty_df = pd.concat([empty_df, combined_df], ignore_index=True)
            empty_df  = empty_df.sum(axis=0).values.T
            try:
                dictDataFrame[dfid].loc[pd.to_datetime(timestamp)] = empty_df
            except:
                pass
        if side == "C":
            self.df_call = dictDataFrame.copy()
        if side == "P": 
            self.df_put = dictDataFrame.copy()     


        
class voidflow():
    """
        Gathers statistical information on canceled limit order books in the form of a heatmap variance over 60 second history
        return pd.Dataframe with columns indicating levels. df contains only 1 row
    """
    def __init__ (self, instrument : str, insType : str, axis : dict):
        """
            axis : A dictionary that encapsulates a collection of flows originating from diverse exchanges in the key "books
                   and the collection of trades origination from different exchanges in the key "trades"
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data
                   should be:
                   {
                      books : classbooks.merge.object
                      trades : classtrades.merge.object
                   }
                   snapshot_voids    : total closed orders per timestamp,  
                                       closed orders heatmap per level, 
                   snapshot_voidsvar : total variance of closed orders
                                       heatmap of variances of closed orders per level
        """
        self.instrument = instrument
        self.insType = insType
        self.axis = axis
        self.snapshot_voids = pd.DataFrame()
        self.snapshot_voidsvar = pd.DataFrame()

    def get_voids(self, current_price):

        
        df_books = self.axis['books'].snapshot.copy()
        df_trades = self.axis['trades'].snapshot.copy()
        merged_df = pd.merge(df_books, df_trades, how='outer', left_index=True, right_index=True, suffixes=('_books', '_trades'))
        common_columns = df_books.columns.intersection(df_trades.columns).tolist()
        
        for column in common_columns:
            if "price" not in column:
                merged_df[column] = merged_df[column + '_books'].sub(merged_df[column + '_trades'], fill_value=0)
                merged_df = merged_df.drop([column + '_books', column + '_trades'], axis=1)
        merged_df = merged_df.drop('price_books', axis=1)
        merged_df = merged_df.rename(columns={'price_trades': 'price'})

        sorted_columns = sorted(map(float, [c for c in merged_df.columns if "price" not in c]))
        merged_df = merged_df[map(str, ['price'] + sorted_columns)] 

        for index in range(len(merged_df)):
            if index != 0:
                merged_df.iloc[index-1] = merged_df.iloc[index].values - merged_df.iloc[index-1].values

        price = merged_df['price'].values[-1]
        self.snapshot_voids = merged_df.drop(columns=['price']).sum(axis=0).T
        snapshot_voids_columns = ["_".join([x.split('.')[0], "void_volume"]) for x in merged_df.columns.tolist()]
        self.snapshot_voids = self.snapshot_voids.rename(columns=dict(zip(self.snapshot_voids.columns, snapshot_voids_columns)))
        total_closed = merged_df.sum(axis=0).sum().values[0]
        self.snapshot_voids.insert(0, 'price', [price])
        self.snapshot_voids.insert(0, 'void_volume', [total_closed])

        self.snapshot_voidsvar = merged_df.var().T
        snapshot_voids_columns = ["_".join([x.split('.')[0], "voidvar_volume"]) for x in merged_df.columns.tolist()]
        self.snapshot_voidsvar = self.snapshot_voids.rename(columns=dict(zip(self.snapshot_voids.columns, snapshot_voids_columns)))
        total_variance = self.snapshot_voidsvar.var().sum().values[0]
        self.snapshot_voids.insert(0, 'price', [price])
        self.snapshot_voidsvar.insert(0, 'voidvar_volume', [total_variance])
        
    