import sys
import time
import json
from os.path import abspath, dirname
from IPython.display import display, clear_output

current_dir = abspath(dirname(__file__))
project_path = abspath(dirname(dirname(current_dir)))
sys.path.append(project_path + "/TradeStreamEngine/StreamEngine")
sys.path.append(project_path + "/TradeStreamEngine/StreamEngineBase")

import exchanges
import lookups

a = exchanges.bybit_flow(20)

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

a.input_from_json()

data = json.load(open("data/bybit_btcusd_perpetual_trades_1.json"))
for t in data:
    t = json.dumps(t)
    print(lookups.bybit_trades_lookup(t))
    a.add_t_perp_usd(t)

print(a.perpetual_axis["trades"]["btcusd"].snapshot_total)