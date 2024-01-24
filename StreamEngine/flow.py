import numpy as np 
import pandas as pd 


from utilis import booksflow_find_level, booksflow_compute_percent_variation, booksflow_manipulate_arrays, booksflow_datatrim

class booksflow():
    """
        Important notes: 
            Maintain consistency in current price and timestamp across all flows
            If the book is above book_ceil_thresh from the current price, it will be omited for the next reasons:
                - Computational efficiency
                - Who books so high if they want to trade now? Challange this statemant ...
            Aggregation explanation:  If the level_size is 20, books between [0-20) go to level 20, [20, 40) go to level 40, and so forth.
    """
    def __init__(self, exchange : str, symbol : str, insType : str, level_size : int, book_ceil_thresh=5):
        """
            insType : spot, future, perpetual 
            level_size : the magnitude of the level to aggragate upon (measured in unites of the quote to base pair)
            book_ceil_thresh : % ceiling of price levels to ommit, default 5%
        """
        # Identification
        self.exchange = exchange
        self.symbol = symbol
        self.insType = insType
        self.level_size = float(level_size)
        self.book_ceil_thresh = book_ceil_thresh
        self.raw_data = pd.DataFrame()
        self.B = {"price" : None, "timestamp" : None, "bids" : {}, "asks" : {}}
        self.snapshot = None
        self.previous_second = -1
        self.current_second = 0

    
    def update_books(self, current_price, books, bids_key_name, asks_key_name, key_timestamp):
        """
            bids_name, asks_name, t_name : Different jsons have different name for bids and asks, timestamp
        """
        self.B['timestamp'] = books[key_timestamp]
        self.B['current_price'] = current_price
        self.update_books_helper(current_price, books[bids_key_name], 'bids')
        self.update_books_helper(current_price, books[asks_key_name], 'asks')
        
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
            # Delete unnecessary data
            booksflow_datatrim(current_price, self.B, 'bids', self.book_ceil_thresh)
            booksflow_datatrim(current_price, self.B, 'asks', self.book_ceil_thresh)
            # Start everything all over again
            self.dfs_input_books(current_price)

    def update_books_helper(self, current_price, books, side):
        """
          side: bids, asks
        """
        # Omit books above 5% from the current price
        for book in books:
            p = float(book[0])
            a = float(book[1])
            if abs(booksflow_compute_percent_variation(p, current_price)) > self.book_ceil_thresh:
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
        levels = [booksflow_find_level(lev, self.level_size) for lev in  prices]
        unique_levels, inverse_indices = np.unique(levels, return_inverse=True)
        group_sums = np.bincount(inverse_indices, weights=amounts)
        columns = [str(col) for col in unique_levels]

        if self.raw_data.empty:
            self.raw_data = pd.DataFrame(0, index=list(range(60)), columns = columns)
            self.raw_data.loc[self.current_second] = group_sums
            sorted_columns = sorted(map(float, self.raw_data.columns))
            self.raw_data = self.raw_data[map(str, sorted_columns)]
        else:
            old_levels = np.array(self.raw_data.columns)
            new_levels = np.setdiff1d(np.array(columns), old_levels)
            full_new_levels = np.concatenate((old_levels, np.setdiff1d(new_levels, old_levels))) 
            for l in new_levels:
                self.raw_data[l] = 0
            sums = booksflow_manipulate_arrays(old_levels, full_new_levels, group_sums)
            self.raw_data.loc[self.current_second] = sums
            sorted_columns = sorted(map(float, self.raw_data.columns))
            self.raw_data = self.raw_data[map(str, sorted_columns)]            

