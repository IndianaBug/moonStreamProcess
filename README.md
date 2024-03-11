# TradeStreamEngine Library

## Overview
The TradeStreamEngine Library is a powerhouse designed to seamlessly process and synthesize real-time financial data streams. It ingests critical market data like order books, trades, liquidations, open interest, and positions, transforming them into actionable insights and a unified view of the market.

## Purpouse
While TradeStreamEngine does not directly generate financial dashboards, its core functionality revolves around aggregating data from various exchanges and providing meticulous one-minute summaries in the form of heatmaps. These heatmaps offer a detailed visualization of market dynamics. TradeStreamEngine's role is pivotal in efficiently aggregating and synthesizing this heatmap data, making it an indispensable tool for traders and analysts seeking real-time insights into market trends. Additionally, it proves invaluable for storing data in non-relational databases, supporting the development of machine learning algorithms, and facilitating the execution of complex analyses

## Core Functionality: Stream Processing Redefined
TradeStreamEngine excels at real-time financial data processing. Here's how it breaks down:
* Modular Design: The library utilizes specialized flow modules as building blocks. Each module meticulously handles a specific data type within the financial stream.
* Stream Aggregation & Synthesis: After the flow modules refine the data, synthesis modules take over. These modules not only consolidate the processed information but also synthesize it into actionable insights, providing a comprehensive market picture.

## Supported Instruments

TradeStreamEngine currently supports only Bitcoin. However, creating support for ETH or any other altcoin shouldn't take much time. The crucial steps are only to change lookups for the data based on the websocket or API you are using and remove unneeded instruments. To get started, follow the instructions provided in the section on making your own streams.

## Supported Exchanges

TradeStreamEngine currently supports the following exchanges for spot/perpetual instruments:

- Binance
- OKX
- Bybit
- KuCoin
- Deribit
- MXC
- Gate.io
- BingX
- Bitget
- Coinbase
- HTX

Additionally, for option instruments, TradeStreamEngine supports:

- Bybit
- Deribit
- OKX

## Important Notes

### Deribit Open Interest

**Caution:** Deribit's Open Interest (OI) values of BTC-PERP may exhibit unusual and abrupt jumps. This behavior could potentially be attributed to API errors. Please use TradeStreamEngine with Deribit on your own risk, and exercise caution when interpreting OI data.

### MEXC API Support

**Attention:** MEXC's API support is reported to have challenges. If you intend to use MEXC data, it is advisable to verify the accuracy of the information. Ensure that the data is not manipulated, as reported Open Interest (OI) values fetched via the API may differ significantly from those reported by external sources such as CoinMarketCap.

**Additional Note on MEXC Perpetual BTCUSDT Books**

**Caution:** The order books for MEXC perpetual BTCUSDT contracts may display unusually high values. This anomaly could be a result of the unregulated nature of the market. Exercise discretion and be aware of potential discrepancies when analyzing MEXC perpetual BTCUSDT books.


# Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Data](#data)
- [Making Streams](#modules)
  - [Flow Modules](#flow-modules)
  - [Lookups Modules](#lookups-modules)
  - [Synthesis Modules](#synthesis-modules)
  - [Assembler Modules](#assembler-modules)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

# Installation

To use the TradeStreamEngine and its modules, follow these installation steps:

1. Clone the repository to your virtual environment library directory:
   ```bash
   git clone https://github.com/badcoder-cloud/TradeStreamEngine
2. Navigate to the project directory:
   ```bash
   cd .../TradeStreamEngine
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt

# Usage

To use the Synthesis Hub with Bitcoin data, follow these steps:
For Python code:

```python
import numpy as np
from StreamEngine.synthHub import btcSynth

level_size = 20
prage = np.array([0.0, 1.0, 2.0, 5.0, 10.0])
expiration_ranges = np.array([0.0, 1.0, 2.0, 5.0, 10.0])
exchanges_spot_perp = ["binance", "okx", "bybit", "bitget", "bingx", "kucoin", "deribit", "coinbase", "htx", "gateio", "mexc"]
exchanges_option = ["bybit", "okx", "deribit"]
abs_path = "" # Absolut path to the directory containing json files. Read method docs

btcDataProcessor = btcSynth(level_size, prage, expiration_ranges, exchanges_spot_perp=exchanges_spot_perp, exchanges_option=exchanges_option)
btcDataProcessor.input_from_json(abs_path) # For testing purpouses to load all of the data at once.
                                           # You may want to fork the code and input the data from unrelational database
btcDataProcessor.merge()
btcDataProcessor.display_full_data()

data = ""
btcDataProcessor.add_okx_perp_btcusd_oi(data) # Will input okx oi. Check for all methods
```

Level Size :

Imagine you're sorting coins. Instead of having a giant pile, this setting groups similar prices together. The "level size" determines the size of these groups. If it's set to 20, then order book and trade, ois and liquidations data will be grouped into buckets that represent price ranges of $20 each.

Price Ranges for Options (pranges):

This setting focuses on option contracts, which give you the right to buy or sell Bitcoin at a specific price by a certain date (expiration). pranges helps categorize options based on their strike price, which is the price at which you can exercise the option to buy or sell.

The example np.array([0.0, 1.0, 2.0, 5.0, 10.0]) creates buckets for options relative to the current Bitcoin price:

10.0: This bucket holds options with a strike price that matches the strikes 10% above the current Bitcoin price (or below if the value is negative). For example if the current price is $10,000, the bucket will encomapass options with strikes above $11,000
0.0:1.0, 1.0:2.0, and so on: These buckets hold options with strike prices that fall within the specified range relative to the current price. For example, if the current price is $10,000, a 1.0:2.0 bucket might encompass options with strike prices between $10,200 and $10,400. It's important to note that negative values, such as -5 and -10, represent options with strike prices below the current price.

Expiration Time Ranges (expiration_ranges):

Similar to price ranges, this setting groups options based on how much time is left until they expire. This helps analyze how option prices change as the expiration date approaches. (Doesn't contain negative ranges)

In Short:

These settings organize complex Bitcoin market data into simpler buckets, making it easier to understand price trends, option behavior based on strike price and expiration time, and ultimately gain valuable insights into the market.

# Data
The data generated is the next:
Example of accessing

``` Python
btcDataProcessor.data.get("spot_books") # to get spot books data
btcDataProcessor.data                   # to access full data
```
Spot Data:
- spot_books: the dictionary of absolute amount of books by aggregated level.
- timestamp: Timestamp of the data.
- spot_buyVol: the total amount of market buys at the current timestamp.
- spot_sellVol: the total amount of market sells at the current timestamp.
- spot_open: the price at the beginning of the 1-minute interval.
- spot_close: the price at the end of the 1-minute interval.
- spot_low: the lowest price within a 1-minute interval.
- spot_high: the highest price within a 1-minute interval.
- spot_Vola: the volatility of the price within a 1-minute interval, calculated using standard deviation.
- spot_VolProfile: the dictionary of market trades (sell + buy) by aggregated level.
- spot_buyVolProfile: the dictionary of market buys by aggregated level.
- spot_sellVolProfile: the dictionary of market sells by aggregated level.
- spot_numberBuyTrades: number of buy trades at the current timestamp.
- spot_numberSellTrades: number of sell trades at the current timestamp.
- spot_orderedBuyTrades: the list of ordered buy trades. (may be usefull for entropy)
- spot_orderedSellTrades: the list of ordered sell trades.
- spot_voids: the dictionary of canceled books by aggregated level. (sums all canceled amounts)
- spot_reinforces: the dictionary of reinforced books by aggregated level. (sums all reinforced amounts)
- spot_totalVoids: the total amount of canceled orders at the current timestamp.
- spot_totalReinforces: the total amount of reinforced orders at the current timestamp.
- spot_voidsDuration: the dictionary of duration of canceled books by aggregated level in seconds. If someone is engaging in spoofing, you can determine the duration for which they place an order before canceling it by measuring the time interval between order placement and cancellation.
- spot_voidsDurationVola: the dictionary of volatility of spot_voidsDuration. 
- spot_reinforcesDuration: the dictionary of duration of reinforced books by aggregated level in seconds.
- spot_reinforcesDurationVola: the dictionary of volatility of spot_reinforcesDuration.

Perpetual Data:
perp_books: Information about perpetual market books.
perp_buyVol: Buy volume in the perpetual market.
perp_sellVol: Sell volume in the perpetual market.
perp_open: Opening price in the perpetual market.
perp_close: Closing price in the perpetual market.
perp_low: Lowest price in the perpetual market.
perp_high: Highest price in the perpetual market.
perp_Vola: Volatility in the perpetual market.
perp_VolProfile: Volume profile in the perpetual market.
perp_buyVolProfile: Buy volume profile in the perpetual market.
perp_sellVolProfile: Sell volume profile in the perpetual market.
perp_numberBuyTrades: Number of buy trades in the perpetual market.
perp_numberSellTrades: Number of sell trades in the perpetual market.
perp_orderedBuyTrades: Ordered buy trades in the perpetual market.
perp_orderedSellTrades: Ordered sell trades in the perpetual market.
perp_voids: Perpetual market voids.
perp_reinforces: Perpetual market reinforces.
perp_totalVoids: Total voids in the perpetual market.
perp_totalReinforces: Total reinforces in the perpetual market.
perp_totalVoidsVola: Total voids volatility in the perpetual market.
perp_voidsDuration: Duration of voids in the perpetual market.
perp_voidsDurationVola: Volatility during voids in the perpetual market.
perp_reinforcesDuration: Duration of reinforces in the perpetual market.
perp_reinforcesDurationVola: Volatility during reinforces in the perpetual market.
perp_weighted_funding: Weighted funding rate in the perpetual market.
perp_total_oi: Total open interest in the perpetual market.
perp_oi_increases: Open interest increases in the perpetual market.
perp_oi_increases_Vola: Volatility during open interest increases in the perpetual market.
perp_oi_decreases: Open interest decreases in the perpetual market.
perp_oi_decreases_Vola: Volatility during open interest decreases in the perpetual market.
perp_oi_turnover: Open interest turnover in the perpetual market.
perp_oi_turnover_Vola: Volatility during open interest turnover in the perpetual market.
perp_oi_total: Total open interest in the perpetual market.
perp_oi_total_Vola: Volatility in total open interest in the perpetual market.
perp_oi_change: Change in open interest in the perpetual market.
perp_oi_Vola: Volatility in open interest in the perpetual market.
perp_orderedOIChanges: Ordered open interest changes in the perpetual market.
perp_OIs_per_instrument: Open interests per instrument in the perpetual market.
perp_fundings_per_instrument: Fundings per instrument in the perpetual market.
liquidations_perp_longsTotal: Total liquidations of long positions in the perpetual market.
liquidations_perp_longs: Liquidations of long positions in the perpetual market.
liquidations_perp_shortsTotal: Total liquidations of short positions in the perpetual market.
liquidations_perp_shorts: Liquidations of short positions in the perpetual market.
TTA_perp_ratio: Ratio of Take-Trader-Action in the perpetual market.
TTP_perp_ratio: Ratio of Take-Trader-Profit in the perpetual market.
GTA_perp_ratio: Ratio of Give-Trader-Action in the perpetual market.
perp_liquidations_longsTotal: Total liquidations of long positions in the perpetual market.
perp_liquidations_longs: Liquidations of long positions in the perpetual market.
perp_liquidations_shortsTotal: Total liquidations of short positions in the perpetual market.
perp_liquidations_shorts: Liquidations of short positions in the perpetual market.
perp_TTA_ratio: Ratio of Take-Trader-Action in the perpetual market.
perp_TTP_ratio: Ratio of Take-Trader-Profit in the perpetual market.
perp_GTA_ratio: Ratio of Give-Trader-Action in the perpetual market.

Option Data:
oi_option_puts_0: Open interest of put options at a strike price of 0 in the perpetual market.
oi_option_puts_10: Open interest of put options at a strike price of 10 in the perpetual market.
oi_option_calls_0: Open interest of call options at a strike price of 0 in the perpetual market.
oi_option_calls_10: Open interest of call options at a strike price of 10 in the perpetual market.








# Key Features:

* Order Book Analysis: Gain a deep understanding of market liquidity, price levels, and order book dynamics.
* Trade Analysis: Extract and analyze market orders (trades) to identify patterns and trends.
* Feature Extraction: Create powerful features from order book data to enhance trading and investment strategies.
* Quantitative Insights: Utilize quantitative metrics and statistical analysis for informed decision-making.


This library works only with seconds, minutes, hours and days

## Formulas

### Books - absolute quantity of order books for certain price levels at certain timestamp.
- $P_i$: The price level at index $i$.
- $Q_i(t)$: The absolute quantity of limit orders at price level \( P_i \) at timestamp \( t \).
- $B_t$ = ${(P_1, Q_1(t)), (P_2, Q_2(t)), ... , (P_n, Q_n(t))}$ : Books at different levels
### Trades - absolute quantity of market trades for certain price levels at certain timestamp.
- $P_i$: The price level at index $i$.
- $Q_i(t)$: The absolute quantity of market orders at price level \( P_i \) at timestamp \( t \).
- $T_t$ = ${(P_1, Q_1(t)), (P_2, Q_2(t)), ... , (P_n, Q_n(t))}$ : Books at different levels
### Canceled limit orders - estimated quantity of canceled limit orders for certain price levels at certain timestamp.
- $D_t$ = ${ (B_t1 + T_t1 ) - ( B_t0 + T_t0 ) }$   -Difference of total placed prders between 2 consegutive timestamps
- $CB_t$ = ${CB_t | CB_t = (D_t - D_t-1) ⋅ [D_t - D_t-1 > 0], ∀_i, 0 ≤ i < n}$ - Total closed orders over a single timestamp 
### Reforced limit orders - estimated quantity of reforced limit orders for certain price levels at certain timestamp.
- $D_t$ = ${ (B_t + T_t ) - ( B_t-1 + T_t-1 ) }$   -Difference of total placed prders between 2 consegutive timestamps
- $CB_t$ = ${CB_t | CB_t = (D_t - D_t-1) ⋅ [D_t - D_t-1 < 0], ∀_i, 0 ≤ i < n}$ - Total closed orders over a single timestamp 


# Create level-stratified heatmaps with length $x$.

![Screenshot 2024-01-23 9 58 27 AM](https://github.com/badcoder-cloud/TradeFeatureEngine/assets/136728020/1ac74fa2-7aff-4fef-a100-7d67ce9894a5)


![Screenshot 2024-01-23 10 01 53 AM](https://github.com/badcoder-cloud/TradeFeatureEngine/assets/136728020/a73545ef-0c58-40aa-924d-64e76bd8e2b8)
