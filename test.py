class mergemaster():
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



class voidflow():
    """
        Gathers statistical information on canceled limit order books in the form of a heatmap variance over 60 second history
        return pd.Dataframe with columns indicating levels. df contains only 1 row
    """
    def __init__ (self, instrument : str, insType : str, axis : dict)
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
        


def build_option_dataframes(expiration_ranges, columns):
    """
        The function is assembling dataframes for options.
        OI for every strike and expiration countdown
    """
    df_dic = {}
    for i, exp_range in enumerate(expiration_ranges):
        if i in [0, len(expiration_ranges)-1]:
            df_dic[f'{int(exp_range)}'] = pd.DataFrame(columns=columns) #.set_index('timestamp')
            df_dic[f'{int(exp_range)}']['timestamp'] = pd.to_datetime([])
            df_dic[f'{int(exp_range)}'].set_index('timestamp', inplace=True)
        if i in [len(expiration_ranges)-1]:
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}'] = pd.DataFrame(columns=columns) #.set_index('timestamp')
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}']['timestamp'] = pd.to_datetime([])
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}'].set_index('timestamp', inplace=True)
        else:
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}'] = pd.DataFrame(columns=columns) #.set_index('timestamp')
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}']['timestamp'] = pd.to_datetime([])
            df_dic[f'{int(expiration_ranges[i-1])}_{int(exp_range)}'].set_index('timestamp', inplace=True)
    df_dic.pop(f"{int(np.max(expiration_ranges))}_{int(np.min(expiration_ranges))}")
    return df_dic

def calculate_option_time_to_expire():
    today_day = datetime.now().timetuple().tm_yday
    today_year = datetime.now().year
    f = datetime.strptime(date, "%d%b%y")
    expiration_date = f.timetuple().tm_yday
    expiration_year = f.year
    if today_year == expiration_year:
        r = expiration_date - today_day
    if today_year == expiration_year + 1:
        r = 365 + expiration_date - today_day
    return float(r)


def deribit_lookup(data : dict, side : str):
  """
      side : P, C
  """
  strikes = np.array([float(x["instrument_name"].split('-')[-2]) for x in data["result"] if x["instrument_name"].split('-')[-1] == logic_side])
  countdowns = np.array([calculate_option_time_to_expire(x["instrument_name"].split('-')[1]) for x in data["result"] if x["instrument_name"].split('-')[-1] == logic_side])
  os = np.array([float(x["open_interest"]) for x in data["result"] if x["instrument_name"].split('-')[-1] == logic_side])
  
  return strikes, countdowns, oi











# For googloe colab

! git clone https://github.com/badcoder-cloud/TradeStreamEngine
import os
os.chdir("/content/TradeStreamEngine")


data = json.load(open("/content/TradeStreamEngine/examples/data/deribit_btcusd_option_OI.json"))

deribitOpOI = oiflowOption("deribit", "btcusd", np.array([1.0, 7.0, 35.0]), deribit_lookup)

deribitOpOI.input_oi(data)