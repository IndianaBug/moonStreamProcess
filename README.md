# Key Features:

* Order Book Analysis: Gain a deep understanding of market liquidity, price levels, and order book dynamics.
* Trade Analysis: Extract and analyze market orders (trades) to identify patterns and trends.
* Feature Extraction: Create powerful features from order book data to enhance trading and investment strategies.
* Quantitative Insights: Utilize quantitative metrics and statistical analysis for informed decision-making.


This library works only with seconds, minutes, hours and days

## Formulas

* Books - absolute quantity of order books for certain price levels at certain timestamp.

- $P_i$: <span style="font-size:10px;">The price level at index</span> $i$.
- $Q_i(t)$: The absolute quantity of orders at price level \( P_i \) at timestamp \( t \).

The mathematical representation for the absolute quantity of order books at a certain timestamp could be written as a set of ordered pairs:
\[ \text{Order Book at timestamp } t = \{(P_1, Q_1(t)), (P_2, Q_2(t)), \ldots, (P_n, Q_n(t))\} \]

Here, \( n \) is the total number of price levels in the order book at timestamp \( t \). Each ordered pair \((P_i, Q_i(t))\) represents a price level \( P_i \) and the absolute quantity \( Q_i(t) \) of orders at that price level at timestamp \( t \).
  
* Trades - absolute quantity of market trades for certain price levels at certain timestamp.
