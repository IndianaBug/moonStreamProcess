class omnify():
    """
       Universal snapshot merger snapshot mergerer, Books, OI, Liquidations, Trades
    """
    def __init__(self, instrument : str, insType : str, axis : dict, flowType : str):
        """
            axis : A dictionary that encapsulates a collection of flows originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
            flowType : books, trades, oi, liquidations

        books: 
            snapshot_lastbooks - 1m price close, open, high, low, price variance, last books of each levels
            snapshot_booksvar - 1m price close, total variance of books, variance of books over 1 minute at each price level
        trades:
            snapshot_trades - 1m price close, 1 min volume traded, trades distribution over each price level
            snapshot_tradesvar - 1m price close, 1 min total variance of books, variance distribution over each price level
        oi : 
            snapshot_oi - 1m price close, total increase/decrease of open interest, distribution of open interest increase decrease over each price level
            snapshot_oivar -  1m price close, total variance of open interest, variance of open interest over each price level
        liquidations:
            snapshot_liquidations - 1m price close, total liquidations over 1 minute, liquidations at each price level
                  
        """
        self.instrument = instrument
        self.insType = insType
        self.flowType = flowType
        self.axis = axis
        self.snapshot = pd.DataFrame()
        self.suffixes = merge_suffixes(len(axis))
        if flowType == 'books':
            self.snapshot_lastbooks = pd.DataFrame()
            self.snapshot_booksvar = pd.DataFrame()
        if flowType == 'trades':
            self.snapshot_trades = pd.DataFrame()
            self.snapshot_tradesvar = pd.DataFrame()
        if flowType == 'oi':
            self.snapshot_oi = pd.DataFrame()
            self.snapshot_oivar = pd.DataFrame()
        if flowType == 'liquidations':
            self.snapshot_liquidations = pd.DataFrame()


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
                if "price" not in column:
                    sum[common_columns] = sum[common_columns] + merged_df[column]

        self.snapshot = sum.copy()
        sorted_columns = sorted(map(float, [c for c in self.snapshot.columns if "price" not in c]))
        self.snapshot = self.snapshot[map(str, ['price'] + sorted_columns)] 


        if self.flowType == 'books':
            
            # Last books
            self.snapshot_lastbooks = self.snapshot.drop(columns=['price']).iloc[-1].T
            open = self.napshot['price'].values[0]
            low = self.snapshot['price'].values.min()
            high = self.snapshot['price'].values.max()
            close = self.snapshot['price'].values[-1]
            price_var = self.snapshot['price'].var()
            for index, col, value in enumerate(zip(['open', 'low', 'high', 'close', 'price_var'], [open, low, high, close, price_var])):
                self.snapshot_lastbooks.insert(loc=index, column=col, value=[value])
            
            # Books var
            self.snapshot_booksvar = self.snapshot.var().T
            renamed_columns = ["".join([col, "_var"]) for col in self.snapshot_booksvar.columns.tolist()]
            self.snapshot_booksvar = self.snapshot_booksvar.rename(columns=dict(zip(self.snapshot_booksvar.columns, renamed_columns)))
            total_var = self.snapshot.var().sum()
            for index, col, value in enumerate(zip(['close', 'books_var'], [close, total_var])):
                self.snapshot_booksvar.insert(loc=index, column=col, value=[value])            


        if self.flowType == 'trades':

            # trades
            price = self.snapshot.copy()['price'].values[-1]
            df = self.snapshot.copy().drop(columns=['price'])
            # Drop columns with only 0s
            df = df.loc[(df != 0).any(axis=1)]
            total_volume = df.sum().sum()
            self.snapshot_trades = df.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_volume])):
                self.snapshot_trades.insert(loc=index, column=col, value=[value])

            # trades variance. Higher variance indicate the presence of block trades
            df = self.snapshot.copy().drop(columns=['price'])
            self.snapshot_tradesvar = df.loc[(df != 0).any(axis=1)].var().T
            total_variance = self.snapshot_trades.sum()
            for index, col, value in enumerate(zip(['price', 'volume_variance'], [price, total_variance])):
                self.snapshot_tradesvar.insert(loc=index, column=col, value=[value])

        if self.flowType == 'oi':

            # oi increase/decrease
            price = self.snapshot.copy()['price'].values[-1]
            df = self.snapshot.copy().drop(columns=['price'])
            df = df.loc[(df != 0).any(axis=1)]
            total_oi = df.sum().sum()
            self.snapshot_oi = df.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
                self.snapshot_oi.insert(loc=index, column=col, value=[value])

            # on increase/decrease variance
            self.snapshot_oivar  = self.snapshot.copy().drop(columns=['price'])
            self.snapshot_oivar  = self.snapshot_oivar.loc[(df != 0).any(axis=1)].var().T
            total_oi = self.snapshot_oivar.sum()
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
                self.snapshot_oivar.insert(loc=index, column=col, value=[value])


        if self.flowType == 'liquidations':
            price = self.snapshot.copy()['price'].values[-1]
            self.snapshot_liquidations  = self.snapshot.copy().drop(columns=['price'])
            total_liquidations = self.snapshot_liquidations.sum().sum()
            self.snapshot_liquidations = self.snapshot_liquidations.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_liquidations])):
                self.snapshot_oivar.insert(loc=index, column=col, value=[value])

class mastermerge():
    """
       Universal snapshot merger snapshot mergerer, Books, OI, Liquidations, Trades
    """
    def __init__(self, instrument : str, insType : str, axis : dict, flowType : str):
        """
            axis : A dictionary that encapsulates a collection of flows originating from diverse exchanges.
                   Each key contains the corresponding 60 second pd.Dataframes of heatmap data from different flows
            flowType : books, trades, oi, liquidations

        books: 
            snapshot_lastbooks - 1m price close, open, high, low, price variance, last books of each levels
            snapshot_booksvar - 1m price close, total variance of books, variance of books over 1 minute at each price level
        trades:
            snapshot_trades - 1m price close, 1 min volume traded, trades distribution over each price level
            snapshot_tradesvar - 1m price close, 1 min total variance of books, variance distribution over each price level
        oi : 
            snapshot_oi - 1m price close, total increase/decrease of open interest, distribution of open interest increase decrease over each price level
            snapshot_oivar -  1m price close, total variance of open interest, variance of open interest over each price level
        liquidations:
            snapshot_liquidations - 1m price close, total liquidations over 1 minute, liquidations at each price level
                  
        """
        self.instrument = instrument
        self.insType = insType
        self.flowType = flowType
        self.axis = axis
        self.snapshot = pd.DataFrame()
        self.suffixes = merge_suffixes(len(axis))
        if flowType == 'books':
            self.snapshot_lastbooks = pd.DataFrame()
            self.snapshot_booksvar = pd.DataFrame()
        if flowType == 'trades':
            self.snapshot_trades = pd.DataFrame()
            self.snapshot_tradesvar = pd.DataFrame()
        if flowType == 'oi':
            self.snapshot_oi = pd.DataFrame()
            self.snapshot_oivar = pd.DataFrame()
        if flowType == 'liquidations':
            self.snapshot_liquidations = pd.DataFrame()


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
                if "price" not in column:
                    sum[common_columns] = sum[common_columns] + merged_df[column]

        self.snapshot = sum.copy()
        sorted_columns = sorted(map(float, [c for c in self.snapshot.columns if "price" not in c]))
        self.snapshot = self.snapshot[map(str, ['price'] + sorted_columns)] 


        if self.flowType == 'books':
            
            # Last books
            self.snapshot_lastbooks = self.snapshot.drop(columns=['price']).iloc[-1].T
            open = self.napshot['price'].values[0]
            low = self.snapshot['price'].values.min()
            high = self.snapshot['price'].values.max()
            close = self.snapshot['price'].values[-1]
            price_var = self.snapshot['price'].var()
            for index, col, value in enumerate(zip(['open', 'low', 'high', 'close', 'price_var'], [open, low, high, close, price_var])):
                self.snapshot_lastbooks.insert(loc=index, column=col, value=[value])
            
            # Books var
            self.snapshot_booksvar = self.snapshot.var().T
            renamed_columns = ["".join([col, "_var"]) for col in self.snapshot_booksvar.columns.tolist()]
            self.snapshot_booksvar = self.snapshot_booksvar.rename(columns=dict(zip(self.snapshot_booksvar.columns, renamed_columns)))
            total_var = self.snapshot.var().sum()
            for index, col, value in enumerate(zip(['close', 'books_var'], [close, total_var])):
                self.snapshot_booksvar.insert(loc=index, column=col, value=[value])            


        if self.flowType == 'trades':

            # trades
            price = self.snapshot.copy()['price'].values[-1]
            df = self.snapshot.copy().drop(columns=['price'])
            # Drop columns with only 0s
            df = df.loc[(df != 0).any(axis=1)]
            total_volume = df.sum().sum()
            self.snapshot_trades = df.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_volume])):
                self.snapshot_trades.insert(loc=index, column=col, value=[value])

            # trades variance. Higher variance indicate the presence of block trades
            df = self.snapshot.copy().drop(columns=['price'])
            self.snapshot_tradesvar = df.loc[(df != 0).any(axis=1)].var().T
            total_variance = self.snapshot_trades.sum()
            for index, col, value in enumerate(zip(['price', 'volume_variance'], [price, total_variance])):
                self.snapshot_tradesvar.insert(loc=index, column=col, value=[value])

        if self.flowType == 'oi':

            # oi increase/decrease
            price = self.snapshot.copy()['price'].values[-1]
            df = self.snapshot.copy().drop(columns=['price'])
            df = df.loc[(df != 0).any(axis=1)]
            total_oi = df.sum().sum()
            self.snapshot_oi = df.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
                self.snapshot_oi.insert(loc=index, column=col, value=[value])

            # on increase/decrease variance
            self.snapshot_oivar  = self.snapshot.copy().drop(columns=['price'])
            self.snapshot_oivar  = self.snapshot_oivar.loc[(df != 0).any(axis=1)].var().T
            total_oi = self.snapshot_oivar.sum()
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_oi])):
                self.snapshot_oivar.insert(loc=index, column=col, value=[value])


        if self.flowType == 'liquidations':
            price = self.snapshot.copy()['price'].values[-1]
            self.snapshot_liquidations  = self.snapshot.copy().drop(columns=['price'])
            total_liquidations = self.snapshot_liquidations.sum().sum()
            self.snapshot_liquidations = self.snapshot_liquidations.sum().T
            for index, col, value in enumerate(zip(['price', 'total_volume'], [price, total_liquidations])):
                self.snapshot_oivar.insert(loc=index, column=col, value=[value])


class OOImerge():
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


