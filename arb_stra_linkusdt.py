# -*- coding: utf-8 -*-
"""
Created on Sat Oct 21 22:31:54 2023

@author: junol
"""

import re
import pandas as pd
import matplotlib.pyplot as plt

bar_bnc = pd.read_csv('bar_bnc.csv')
bar_hb = pd.read_csv('bar_hb.csv')
orderbook_bnc = pd.read_csv('orderbook_bnc.csv')
orderbook_hb = pd.read_csv('orderbook_hb.csv')

target_symbol = 'LINKUSDT.BNC'
target_symbol_hb = 'LINKUSDT.HB'

#Order book data processing
comb_orderbook = pd.concat([orderbook_bnc, orderbook_hb])
comb_orderbook = pd.melt(comb_orderbook, id_vars = ['timestamp', 'symbol'], var_name = 'type', value_name = 'value')
comb_orderbook['type_gp'] = comb_orderbook['type'].apply(lambda x: re.sub(r'\d', '', x))


comb_orderbook_price = comb_orderbook[~comb_orderbook['type_gp'].str.contains('v')]
comb_orderbook_price = comb_orderbook_price.drop(['type'], axis = 1)
comb_orderbook_price = comb_orderbook_price.rename(columns = {'value': 'price'})
comb_orderbook_price = comb_orderbook_price.reset_index(drop = True)

comb_orderbook_volume = comb_orderbook[comb_orderbook['type_gp'].str.contains('v')]
comb_orderbook_volume = comb_orderbook_volume.rename(columns = {'value': 'depth'})
comb_orderbook_volume = comb_orderbook_volume.reset_index(drop = True)

comb_orderbook = pd.concat([comb_orderbook_price, comb_orderbook_volume['depth']], axis = 1)
comb_orderbook_ask = comb_orderbook[comb_orderbook['type_gp'] == 'a']
comb_orderbook_ask['adj_price'] = comb_orderbook_ask['price'] * [1 + 1/1000 if x == target_symbol else 1 + 2/1000 for x in comb_orderbook_ask['symbol']]
comb_orderbook_bid = comb_orderbook[comb_orderbook['type_gp'] == 'b']
comb_orderbook_bid['adj_price'] = comb_orderbook_bid['price'] * [1 - 1/1000 if x == target_symbol else 1 - 2/1000 for x in comb_orderbook_bid['symbol']]

best_ask = comb_orderbook_ask.groupby('timestamp', as_index = False)['adj_price'].min().rename(columns = {'adj_price': 'best_ask'})
best_bid = comb_orderbook_bid.groupby('timestamp', as_index = False)['adj_price'].max().rename(columns = {'adj_price': 'best_bid'})
best_price = pd.merge(best_bid, best_ask, on = 'timestamp')

comb_orderbook_ask = pd.merge(comb_orderbook_ask, best_price, on = 'timestamp')
comb_orderbook_bid = pd.merge(comb_orderbook_bid, best_price, on = 'timestamp')

#Calculate the arbitrage opportunity, after fee deduction, assuming the highest fee, HB 0.2% and BNC 0.1%
comb_orderbook_ask['arbable'] = comb_orderbook_ask['adj_price'] < comb_orderbook_ask['best_bid']

#Seems there are no arbitrage opportunity if fee is counted
arbable_with_fee = comb_orderbook_ask[comb_orderbook_ask['arbable']]

# #Let's go back to the no fees assumption
comb_orderbook_ask = comb_orderbook[comb_orderbook['type_gp'] == 'a']
comb_orderbook_bid = comb_orderbook[comb_orderbook['type_gp'] == 'b']
best_ask = comb_orderbook_ask.groupby('timestamp', as_index = False)['price'].min().rename(columns = {'price': 'best_ask'})
best_bid = comb_orderbook_bid.groupby('timestamp', as_index = False)['price'].max().rename(columns = {'price': 'best_bid'})
best_price = pd.merge(best_bid, best_ask, on = 'timestamp')

comb_orderbook_ask = pd.merge(comb_orderbook_ask, best_price, on = 'timestamp')
comb_orderbook_bid = pd.merge(comb_orderbook_bid, best_price, on = 'timestamp')


comb_orderbook_ask['arbable'] = comb_orderbook_ask['price'] < comb_orderbook_ask['best_bid']
arbable_ask = comb_orderbook_ask[comb_orderbook_ask['arbable']].reset_index(drop = True)
comb_orderbook_bid['arbable'] = comb_orderbook_bid['price'] > comb_orderbook_ask['best_ask']
arbable_bid = comb_orderbook_bid[comb_orderbook_bid['arbable']].reset_index(drop = True)

#Get the arbitrage case by looping the orderbook
real_arbable = None
for i in arbable_ask['timestamp'].unique():
    target_ask = arbable_ask[arbable_ask['timestamp'] == i]
    target_bid = arbable_bid[arbable_bid['timestamp'] == i]
    while min(target_ask.shape[0], target_bid.shape[0]) > 0:
        target_depth = min(target_ask['depth'].iloc[0], target_bid['depth'].iloc[0])
        target_arbable_ask = target_ask.iloc[:1]
        target_arbable_ask['depth'] = target_depth
        target_arbable_bid = target_bid.iloc[:1]
        target_arbable_bid['depth'] = target_depth
        real_arbable = pd.concat([real_arbable, target_arbable_ask, target_arbable_bid])
        target_ask['depth'].iloc[0] = target_ask['depth'].iloc[0] - target_depth
        target_ask = target_ask[target_ask['depth'] > 0]
        target_bid['depth'].iloc[0] = target_bid['depth'].iloc[0] - target_depth
        target_bid = target_bid[target_bid['depth'] > 0]
     
#Calculate the profit of each exchange then combine
real_arbable['sized_depth'] = real_arbable['depth'] * [1 if x == 'a' else -1 for x in real_arbable['type_gp']]
real_arbable_hb = real_arbable[real_arbable['symbol'] == target_symbol_hb]
real_arbable_hb = pd.merge(real_arbable_hb, bar_hb[['timestamp', 'close']], on = 'timestamp', how = 'left')
real_arbable_hb['close'] = real_arbable_hb['close'].ffill()
real_arbable_bnc = real_arbable[real_arbable['symbol'] == target_symbol]
real_arbable_bnc = pd.merge(real_arbable_bnc, bar_bnc[['timestamp', 'close']], on = 'timestamp', how = 'left')
real_arbable_bnc['close'] = real_arbable_bnc['close'].ffill()

def cal_profit(real_arbable_ex):
    real_arbable_ex['pos'] = real_arbable_ex['sized_depth'].cumsum()
    real_arbable_ex['lag_close'] = real_arbable_ex['close'].shift(1)
    real_arbable_ex['lag_pos'] = real_arbable_ex['pos'].shift(1)
    real_arbable_ex['cost'] = real_arbable_ex['sized_depth'] * real_arbable_ex['price']
    real_arbable_ex['cum_cost'] = real_arbable_ex['cost'].cumsum()
    trading_pnl = real_arbable_ex['sized_depth'] * (real_arbable_ex['close'] - real_arbable_ex['price'])
    position_pnl = real_arbable_ex['lag_pos'] * (real_arbable_ex['close'] - real_arbable_ex['lag_close'])
    real_arbable_ex['profit'] = trading_pnl + position_pnl
    return real_arbable_ex

real_arbable_hb = cal_profit(real_arbable_hb)
real_arbable_bnc = cal_profit(real_arbable_bnc)

# real_arbable_hb_stat = real_arbable_hb.groupby('timestamp')[['cost', 'profit']].sum()
# real_arbable_bnc_stat = real_arbable_bnc.groupby('timestamp')[['cost', 'profit']].sum()

real_arbable_wt_profit = pd.concat([real_arbable_hb, real_arbable_bnc])
real_arbable_stat = real_arbable_wt_profit.groupby('timestamp', as_index = False)[['cost', 'profit']].sum()
real_arbable_stat['cumcost'] = -real_arbable_stat['cost'].cumsum()
real_arbable_stat['cumprofit'] = real_arbable_stat['profit'].cumsum()

#plot equity curve
test = real_arbable_stat['cumcost']
a = [1,2,3]

plt.plot(real_arbable_stat['timestamp'], real_arbable_stat['cumcost'])
plt.xlabel("Time")
plt.ylabel("PnL (USDT)")
plt.title("PnL by Cost (Spot)")
plt.show()

plt.plot(real_arbable_stat['timestamp'], real_arbable_stat['cumprofit'])
plt.xlabel("Time")
plt.ylabel("PnL (USDT)")
plt.title("PnL by Profit (Futures)")
plt.show()
