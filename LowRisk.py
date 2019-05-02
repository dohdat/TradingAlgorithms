# From https://www.quantopian.com/posts/3-dot-92-sharpe-on-2-dot-8-leverage-with-transaction-costs

import math
import numpy as np
import pandas as pd
import scipy as sp
from sklearn.decomposition import PCA
from sklearn.covariance import OAS, EmpiricalCovariance
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.vector_ar.var_model import VAR
import statsmodels.api as smapi
from sklearn.linear_model import LassoCV, LinearRegression
from sklearn.svm import SVR
from cvxopt import matrix, solvers
       
def initialize(context):

    # All sectors from https://www.quantopian.com/help/fundamentals#industry-sector
    # 
    context.sector_ids = [101,102,103,104,
                          205,206,207,
                          308,309,310,311]
    
    context.leverage = 2.5
    context.days = 15
    
    schedule_function(trade_sectors, 
                      date_rules.every_day(), 
                      time_rules.market_open(minutes=10))

    schedule_function(update_chart, 
                      date_rules.every_day(), 
                      time_rules.market_open(minutes=60))
    set_commission(commission.PerShare(cost=0.005, min_trade_cost=1.00))        
def trade_sectors(context, data):
    context.sec_weights = {}
    
    for sector_id in context.sector_ids:
        context.sec_weights[sector_id] = find_weights(context, data, context.sectorStocks[sector_id])
    
    weights = pd.concat(context.sec_weights.values())
    if weights.abs().sum() > 0:
        weights /= weights.abs().sum()
        weights *= context.leverage

    context.weights = weights
        
    for eq in weights.index:
        if data.can_trade(eq):
            order_target_percent(eq, weights[eq])

    for eq in context.portfolio.positions:
        if eq not in context.weights:
            if data.can_trade(eq):
                order_target(eq, 0)
                
def before_trading_start(context, data):
    
    if context.days < 15:
        context.days += 1
        return
    context.days = 0
    
    context.sectorStocks = {}    
    
    for sec_id in context.sector_ids:
        fundamental_df = get_fundamentals(query(fundamentals.valuation.market_cap)
            .filter(fundamentals.asset_classification.morningstar_sector_code == sec_id)
            .filter(fundamentals.valuation.market_cap != None)
            .filter(fundamentals.company_reference.primary_exchange_id != "OTCPK") 
            .filter(fundamentals.share_class_reference.security_type == 'ST00000001') 
            .filter(~fundamentals.share_class_reference.symbol.contains('_WI')) 
            .filter(fundamentals.share_class_reference.is_primary_share == True) 
            .filter(fundamentals.share_class_reference.is_depositary_receipt == False) 
            .filter(fundamentals.operation_ratios.roe > 0)
            .order_by(fundamentals.valuation.market_cap.desc())).T
        context.sectorStocks[sec_id] = fundamental_df[0:30].index
    
def find_weights(context, data, stocks):
    prices = data.history(stocks, "price", 400, "1d")
    prices = prices.dropna(axis=1)
    logP = np.log(prices.values)
    diff = np.diff(logP, axis=0)
    cov = EmpiricalCovariance().fit(diff).covariance_
    e, v = sp.linalg.eigh(cov)
    idx = e.argsort()
    loadings = np.dot(v[:, idx[-5:]].real, np.diag(np.sqrt(e[-5:])))
    factors = np.dot(loadings.T, logP.T).T / np.shape(logP)[0]
    residuals = smapi.OLS(logP, factors).fit().resid
    
    signal = residuals[-1, :]
    W, status = getW(cov, signal)
    
    if status <> 'optimal':
        return []
    
    sec_weights = pd.Series(0., index=stocks)
    
    for i, sid in enumerate(prices.columns):
        if data.can_trade(sid):
            sec_weights[sid] = -W[i] / np.sum(np.abs(W)) * 0.7
            
    return sec_weights       

def getW(cov, signal):
    P = matrix(cov)
    (m, m) = np.shape(cov)
    q = matrix(-signal, (m, 1))
    A = matrix([1.] * m, (1, m))
    b = matrix(0., (1,1))
    res = solvers.qp(P=P, q=q, A=A, b=b)
    return res['x'], res['status']

def update_chart(context,data):
    record(leverage = context.account.leverage)
    # record(opt_dd=context.opt_dd)
    # log.debug("Leverage: %f" % (context.account.leverage))

    longs = shorts = 0
    long_lever = 0
    short_lever = 0
    
    for position in context.portfolio.positions.itervalues():        
        if position.amount > 0:
            longs += 1
            
            long_lever += position.amount*data.current(position.sid, 'price')
        if position.amount < 0:
            shorts += 1
            short_lever += -position.amount*data.current(position.sid, 'price')
            
    #record(long_count=longs, short_count=shorts)
    long_lever /= context.portfolio.portfolio_value
    short_lever /= context.portfolio.portfolio_value
    if 'weights' in context:
        record(target_lev=context.weights.abs().sum())
    record(long_lever=long_lever, short_lever=short_lever)
