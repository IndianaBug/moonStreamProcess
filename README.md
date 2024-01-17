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
- $D_t$ = ${ (B_t + T_t ) - ( B_t-1 + T_t-1 ) }$   -Difference of total placed prders between 2 consegutive timestamps
- $CB_t$ = ${CB_t | CB_t = (D_t - D_t-1) ⋅ [D_t - D_t-1 > 0], ∀_i, 0 ≤ i < n}$ - Total closed orders over a single timestamp 
### Reforced limit orders - estimated quantity of reforced limit orders for certain price levels at certain timestamp.
- $D_t$ = ${ (B_t + T_t ) - ( B_t-1 + T_t-1 ) }$   -Difference of total placed prders between 2 consegutive timestamps
- $CB_t$ = ${CB_t | CB_t = (D_t - D_t-1) ⋅ [D_t - D_t-1 < 0], ∀_i, 0 ≤ i < n}$ - Total closed orders over a single timestamp 

