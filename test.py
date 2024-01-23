import json 
import pandas as pd
import numpy as np
import math

books = json.load(open("/content/SatoshiVault/examples/data/binance_perp/books.json"))
books_updates = json.load(open("/content/SatoshiVault/examples/data/binance_perp/bupdates.json"))

def find_level(price, level_size):
    return np.ceil(price / level_size) * level_size

def compute_percent_variation(new_value, old_value):
    try:
        percentage_difference = ((new_value - old_value) / abs(old_value)) * 100
        return percentage_difference
    except:
        return 9999999999

compute_percent_variation(2, 5)



class bookflow():
    """
        Important notes: 
            Keep current price and current timestamp consistent among all of the sProcessors
            If the book is above book_ceil_thresh from the current price, it will be omited for computational efficiency.
            It would be wise to assume that over 60 secods, very wide books are unimportant 
            Aggregation explanation: if the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40 ...
    """
    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, book_ceil_thresh=5):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon 
            book_ceil_thresh : % ceiling of price levels to ommit, default 5%
        """
        # Identification
        self.exchange = exchange
        self.symbol = symbol
        self.level_size = float(level_size)
        self.book_ceil_thresh = book_ceil_thresh
        self.raw_data = pd.DataFrame()
        self.B = {"price" : None, "timestamp" : None, "bids" : {}, "asks" : {}}
        self.snapshot = None
        self.previous_second = 0
        self.current_second = 1

    
    def update_books(self, current_price, key_timestamp, total_books, bids_key_name, asks_key_name):
        """
            bids_name, asks_name, t_name : Different jsons have different name for bids and asks, timestamp
        """
        self.B['timestamp'] = total_books[key_timestamp]
        self.B['current_price'] = current_price
        self.update_books_helper(current_price, total_books[bids_key_name], 'bids')
        self.update_books_helper(current_price, total_books[asks_key_name], 'asks')
        
        self.current_second = int(self.B['timestamp'] % 60) 

        if self.current_second > self.previous_second:
            self.dfs_input_books(current_price)
            self.previous_second = self.current_second
        if self.previous_second > self.current_second:
            self.raw_data.replace(0, method='ffill', inplace=True)     
            self.raw_data.replace(0, method='bfill', inplace=True)
            self.snapshot = self.raw_data.copy()
            self.raw_data = pd.DataFrame()
            self.previous_second = self.current_second
            self.dfs_input_books(current_price)

    def update_books_helper(self, current_price, books, side):
        """
          side: bids, asks
        """
        # Omit books above 5% from the current price
        for book in books:
            p = float(book[0])
            a = float(book[1])
            if abs(compute_percent_variation(p, current_price)) > self.book_ceil_thresh:
                continue
            if a == 0:
                try:
                    del self.B[side][book[0]]
                except:
                    pass
            else:
                self.B[side][p] = a

    def dfs_input_books(self, current_price):
        """
            Inputs bids and asks into dfs
        """

        prices = np.array(list(map(float, self.B['bids'].keys())) + list(map(float, self.B['asks'].keys())), dtype=np.float16)
        amounts = np.array(list(map(float, self.B['bids'].values())) + list(map(float, self.B['asks'].values())), dtype=np.float16)
        levels = [find_level(lev, self.level_size) for lev in  prices]

        if self.raw_data.empty:
            self.raw_data = pd.DataFrame(0, index=list(range(60), columns = [str(col) for col in levels]))
            unique_levels, inverse_indices = np.unique(levels, return_inverse=True)
            group_sums = np.bincount(inverse_indices, weights=amounts)
            self.raw_data.loc[self.current_second] = group_sums
        else:
            sums = pd.DataFrame(0, index=list(range(60), columns = [str(col) for col in levels]))
            unique_levels, inverse_indices = np.unique(levels, return_inverse=True)
            group_sums = np.bincount(inverse_indices, weights=amounts)
            sums[self.current_second] = group_sums           
            self.raw_data = pd.concat([self.raw_data, sums], axis=1)
#            self.raw_data = self.raw_data.sort_index(axis=1)


        unique_levels, inverse_indices = np.unique(levels, return_inverse=True)
        group_sums = np.bincount(inverse_indices, weights=amounts)

        result = np.column_stack((unique_levels, group_sums))

        print(result)



        # columns = [str(x) for x in levels_bins]

        # # Handle dfs columns
        # if self.raw_data.empty:
        #     self.raw_data = pd.DataFrame({"levels" : columns, "amounts" : amounts}).groupby("levels").sum().T.reset_index()
        #     self.raw_data.index = [self.current_second]
        #     self.raw_data = self.raw_data.sort_index(axis=1)
        # else:
        #     sums  = pd.DataFrame({"levels" : columns, "amounts" : amounts}).groupby("levels").sum().T.reset_index()
        #     sums.index = [self.current_second]
        #     self.raw_data = pd.merge(self.raw_data, sums, how='outer').fillna(0)
        #     self.raw_data = self.raw_data.sort_index(axis=1)









books = json.load(open("/content/SatoshiVault/examples/data/binance_perp/books.json"))
books_updates = json.load(open("/content/SatoshiVault/examples/data/binance_perp/bupdates.json"))

btc_price = (float(books['bids'][0][0]) + float(books['asks'][0][0])) / 2




a  = bookflow('binance', 'btc_usdt', btc_price, 20)
start = time.time()
a.update_books(btc_price, 'timestamp',  books, 'bids', 'asks')
a.dfs_input_books(btc_price)
for e in books_updates:
    a.update_books(btc_price, 'timestamp',  e, 'b', 'a')
    a.dfs_input_books(btc_price)

print(f"elapsed_time for {len(books_updates)+1} iterations: ", time.time() - start)
a.raw_data 