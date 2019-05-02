from quantopian.pipeline import Pipeline
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.factors import SimpleMovingAverage, AverageDollarVolume
from quantopian.pipeline.filters.morningstar import IsPrimaryShare
import numpy as np
import pandas as pd
import datetime
import pytz
def initialize(context):
    #set_commission(commission.PerShare(cost=0.01, min_trade_cost=0.00))
    set_slippage(slippage.VolumeShareSlippage(volume_limit=.20, price_impact=0.0))
    #set_slippage(slippage.FixedSlippage(spread=0.00))
    set_commission(commission.PerTrade(cost=0.00))
    #set_slippage(slippage.FixedSlippage(spread=0.00))
    #set_long_only()

    context.MyNumberOfPositions=30
    context.MyLeastPrice=1.25
    context.MyMostPrice=1.75
    context.MaxPositionFactor=1.00/context.MyNumberOfPositions
    context.assets=[sid(8554),sid(32268)]
    context.inverse= [sid(32268), sid(39212)]
    context.safe =   [ sid(41709),  # TLT 20+ Year T Bonds
                       sid(32133),
                       sid(26807),
                       sid(23921),
                       sid(23870)]  # IEF 7-10 Year T Notes
    context.secs =   [ 
                       sid(37514), # Daily Bull  
                       sid(37049), #Financial Bull
                       sid(37515), #Small cap bull
                       sid(37945), #mid cap bull
                       sid(37854), #tech bull
                       sid(38533), #SP500
                       sid(43180), #Financial Bull
                       sid(39216), #midcap 400
                       sid(41575), #basic materials bull
                       sid(38596), #real estate bull
                       sid(37852), #emerging market bull
                       sid(39017), #China Bull
                       sid(37513), #Energy Bull
                       sid(49073), #Biotech bull
                       sid(41578), #healthcare bull
                       sid(39343), #semiconductor bull
                       sid(3766),#IBM
                       sid(8151),#WFC
                       sid(17702),#NLY
                       sid(6935),#SLM
                       ]  # XLU Utilities SPRD Fund
    context.stop_price= 0                 
    # Rebalance
    context.RepeatThisManyTimes=5
    context.EveryThisManyMinutes=43
    for minutez in xrange(
        7, 
        context.EveryThisManyMinutes*context.RepeatThisManyTimes, 
        context.EveryThisManyMinutes
    ):
       schedule_function(first, date_rules.every_day(), time_rules.market_open(minutes=minutez))   

    # Prevent excessive logging of canceled orders at market close.
    schedule_function(cancel_open_orders, date_rules.every_day(), time_rules.market_close(hours=0, minutes=1))

    # Record variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
    #schedule_function(sell_everything, date_rules.every_day(),time_rules.market_close(hours=0,minutes=5))

    # Create our pipeline and attach it to our algorithm.
    my_pipe = make_pipeline(context)
    attach_pipeline(my_pipe, 'my_pipeline')

def make_pipeline(context):
    """
    Create our pipeline.
    """

    # Filter for primary share equities. IsPrimaryShare is a built-in filter.
    primary_share = IsPrimaryShare()

    # Equities listed as common stock (as opposed to, say, preferred stock).
    # 'ST00000001' indicates common stock.
    common_stock = morningstar.share_class_reference.security_type.latest.eq('ST00000001')

    # Non-depositary receipts. Recall that the ~ operator inverts filters,
    # turning Trues into Falses and vice versa
    not_depositary = ~morningstar.share_class_reference.is_depositary_receipt.latest

    # Equities not trading over-the-counter.
    not_otc = ~morningstar.share_class_reference.exchange_id.latest.startswith('OTC')

    # Not when-issued equities.
    not_wi = ~morningstar.share_class_reference.symbol.latest.endswith('.WI')

    # Equities without LP in their name, .matches does a match using a regular
    # expression
    not_lp_name = ~morningstar.company_reference.standard_name.latest.matches('.* L[. ]?P.?$')

    # Equities with a null value in the limited_partnership Morningstar
    # fundamental field.
    not_lp_balance_sheet = morningstar.balance_sheet.limited_partnership.latest.isnull()

    # Equities whose most recent Morningstar market cap is not null have
    # fundamental data and therefore are not ETFs.
    have_market_cap = morningstar.valuation.market_cap.latest.notnull()

    # At least a certain price
    price = USEquityPricing.close.latest
    AtLeastPrice   = (price >= context.MyLeastPrice)
    AtMostPrice    = (price <= context.MyMostPrice)

    # Filter for stocks that pass all of our previous filters.
    tradeable_stocks = (
        primary_share
        & common_stock
        & not_depositary
        & not_otc
        & not_wi
        & not_lp_name
        & not_lp_balance_sheet
        & have_market_cap
        & AtLeastPrice
        & AtMostPrice
    )

    LowVar=6
    HighVar=40

    log.info('\nAlgorithm initialized variables:\n context.MyNumberOfPositions %s \n LowVar %s \n HighVar %s'
        % (context.MyNumberOfPositions, LowVar, HighVar)
    )

    # High dollar volume filter.
    base_universe = AverageDollarVolume(
        window_length=20,
        mask=tradeable_stocks
    ).percentile_between(LowVar, HighVar)

    # Short close price average.
    ShortAvg = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=3,
        mask=base_universe
    )

    # Long close price average.
    LongAvg = SimpleMovingAverage(
        inputs=[USEquityPricing.close],
        window_length=45,
        mask=base_universe
    )

    percent_difference = (ShortAvg - LongAvg) / LongAvg

    # Filter to select securities to long.
    stocks_worst = percent_difference.bottom(context.MyNumberOfPositions)
    securities_to_trade = (stocks_worst)

    return Pipeline(
        columns={
            'stocks_worst': stocks_worst
        },
        screen=(securities_to_trade),
    )

def my_compute_weights(context):
    """
    Compute ordering weights.
    """
    # Compute even target weights for our long positions and short positions.
    stocks_worst_weight = 1.00/len(context.stocks_worst)
    
    
    return stocks_worst_weight

def before_trading_start(context, data):
       
    # Gets our pipeline output every day.
    context.output = pipeline_output('my_pipeline')

    context.stocks_worst = context.output[context.output['stocks_worst']].index.tolist()

    context.stocks_worst_weight = my_compute_weights(context)
    pass
def first(context, data):
    ############# Calculate spy_z ###############
    spy_price_history = data.history(sid(8554), "price", 70, "1d")[:-1]
    spy_mean = spy_price_history.mean()
    spy_sigma = spy_price_history.std()
    spy_price = data.current(sid(8554), "price")
    spy_z = (spy_price - spy_mean) / spy_sigma if spy_sigma !=0 else 0
    record(spy_z = spy_z) 
    ############################################
    
    ###############Calculate Slope##############
    for stock in context.assets:
     prices_spy = data.history(sid(8554), "price",70 , '1d')[0:-1]
     ret_sum_spy = prices_spy.pct_change().sum()
     prices_sh= data.history(sid(32268), "price", 70, '1d')[0:-1]
     ret_sum_sh=prices_sh.pct_change().sum()
    
     r_diff = ret_sum_spy - ret_sum_sh
     r_sum = abs(ret_sum_spy) + abs(ret_sum_sh)
     r_diff_norm = r_diff/r_sum
    
     record(r_diff_norm = r_diff_norm)
    ########################################### 
    #####Risk ON/OFF###############
    for stock in context.inverse:
        if get_open_orders(stock): 
            pending_orders = True
            continue         
        if spy_z < -1.0:
            order_target_percent(stock,0.40)
        elif spy_z<-0.8: 
            order_target_percent(stock,0.35)
        elif spy_z<-0.6:
            order_target_percent(stock,0.25)
        elif spy_z<-0.4 and r_diff_norm == -1:
            order_target_percent(sid(23921),0.15)
        elif spy_z<-0.15 and r_diff_norm == -1:
            order_target_percent(sid(23921),0.10)
        else:
            order_target_percent(stock,0.0) 
            
    ###################################
    TakeProfitFactor=1.10
    GetOutFactor=0.95
    GetInFactor=0.97
    cash=context.portfolio.cash
    MaxPositionValue=context.portfolio.portfolio_value*context.MaxPositionFactor
    # Maybe Take Profit or Get Out because no longer in pipeline
    for stock in context.portfolio.positions:
        if stock not in context.safe and stock not in context.secs:
         if data.can_trade(stock):
            Curr_P = float(data.current([stock], 'price'))
            if (
                Curr_P>context.portfolio.positions[stock].cost_basis*TakeProfitFactor
                or
                stock not in context.stocks_worst
                or spy_z < -1.0 and r_diff_norm==-1
            ):
                if get_open_orders(stock):
                    cancel_open_order(stock)
                StockShares = context.portfolio.positions[stock].amount/context.RepeatThisManyTimes
                if StockShares <1:
                    StockShares=1
                StockSharesValue = StockShares * Curr_P
                order(stock, -StockShares,
                    style=LimitOrder(Curr_P* GetOutFactor)
                )
                cash += StockSharesValue

    if cash >= 100:
        for stock in context.stocks_worst: 
            if data.can_trade(stock):
                Curr_P = float(data.current([stock], 'price'))
                if (Curr_P>=context.MyLeastPrice and spy_z>-1.0):
                    StockShares = context.stocks_worst_weight*cash/Curr_P/context.RepeatThisManyTimes
                    if StockShares<1:
                        StockShares=1
                    if (
                        MaxPositionValue>context.portfolio.positions[stock].amount*context.portfolio.positions[stock].cost_basis
                        and
                        cash>StockShares*Curr_P*GetInFactor
                        
                    ):
                        order(stock, StockShares,
                            style=LimitOrder(Curr_P*GetInFactor)
                        )
                        cash -= StockShares*Curr_P*GetInFactor
   
     ###################################
    hist = data.history(context.secs, "price", 70, "1d")[0:-1]
      
    for stock in context.secs:
       if data.can_trade(stock):
        price_history = hist[stock]
        mean = price_history.mean()
        sigma = price_history.std()
        current_price = data.current(stock, "price")        
        sect_z = (current_price - mean) / sigma if sigma!=0 else 0
        StockShares = context.portfolio.positions[stock].cost_basis
        if get_open_orders(stock):
            pending_orders = True 
            continue
        ## sector trade logic   
        if (1.0>spy_z>-1.0 and r_diff_norm<0
            or sect_z<spy_z and -1.0<sect_z<1.0):
            order_target_percent(stock,0.03,style=LimitOrder(current_price))          
        else:          
            order_target_percent(stock,0) 
                                 
def my_record_vars(context, data):
    """
    Record variables at the end of each day.
    """

    # Record our variables.
    record(leverage=context.account.leverage)
    longs = 0
    for position in context.portfolio.positions.itervalues():
        if position.amount > 0:
            longs += 1
        #if position.amount < 0:
            #shorts += 1
    record(long_count=longs) #short_count=shorts)


def log_open_order(StockToLog):
    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.iteritems():
        if stock == StockToLog:
            for order in orders:
                message = 'Found open order for {amount} shares in {stock}'
                log.info(message.format(amount=order.amount, stock=stock))

def log_open_orders():
    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.iteritems():
        for order in orders:
            message = 'Found open order for {amount} shares in {stock}'
            log.info(message.format(amount=order.amount, stock=stock))

def cancel_open_order(StockToCancel):
    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.iteritems():
        if stock == StockToCancel:
            for order in orders:
                #message = 'Canceling order of {amount} shares in {stock}'
                #log.info(message.format(amount=order.amount, stock=stock))
                cancel_order(order)
def cancel_open_orders(context, data):
    oo = get_open_orders()
    if len(oo) == 0:
        return
    for stock, orders in oo.iteritems():
        for order in orders:
            #message = 'Canceling order of {amount} shares in {stock}'
            #log.info(message.format(amount=order.amount, stock=stock))
            cancel_order(order)

# This is the every minute
'''
def sell_everything(context,data):
    GetInFactor=1.00
    for stock in context.portfolio.positions:
       if get_open_orders(stock):
        if context.portfolio.positions[stock]>0:
             order_percent(stock,0,style=LimitOrder())
'''             
def handle_data(context, data):
    pass
