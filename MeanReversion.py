import numpy as np
import pandas as pd
from quantopian.pipeline import Pipeline
from quantopian.pipeline import CustomFactor
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import SimpleMovingAverage, AverageDollarVolume
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.filters import Q500US

def initialize(context):
#============================================================================================   
#============================================================================================    
    context.Gold = 2000.00
#============================================================================================      
#============================================================================================        
    set_long_only()
    set_benchmark(sid(8554))
    #orignially these were CLOSE: hours = 5
    schedule_function(ANALYZE,date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 30))
    schedule_function(sell,date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 30))
    schedule_function(buy,date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 30))
    schedule_function(Local_Maximize,date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 35))
    schedule_function(GLOBAL_Maximize,date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 40))
    schedule_function(day_end,date_rules.every_day(), time_rules.market_close())
    set_slippage(slippage.FixedSlippage(spread=0.0))
    #set_commission(commission.PerShare(cost=0, min_trade_cost=0))
    context.nq=5
    context.nq_vol=3
    my_pipe = make_pipeline()
    attach_pipeline(my_pipe, 'my_pipeline')
    context.max_leverage = [0]
    context.ANV = []
    context.buy = []
    context.sell = []
    context.ETN_buy = []
    context.AmountHeldData = []
    context.B = []
    context.S = []
    context.DayCounter = 0.0
    context.L = 1.0
    context.Maximize = True
    
class Volatility(CustomFactor):  
    inputs = [USEquityPricing.close]
    window_length=132
    
    def compute(self, today, assets, out, close):

        daily_returns = np.log(close[1:-6]) - np.log(close[0:-7])
        out[:] = daily_returns.std(axis = 0)           

class Liquidity(CustomFactor):   
    inputs = [USEquityPricing.volume, morningstar.valuation.shares_outstanding] 
    window_length = 1
    
    def compute(self, today, assets, out, volume, shares):       
        out[:] = volume[-1]/shares[-1]        
        
class Sector(CustomFactor):
    inputs=[morningstar.asset_classification.morningstar_sector_code]
    window_length=1
    
    def compute(self, today, assets, out, sector):
        out[:] = sector[-1]   
        
        
def make_pipeline():
    sector = morningstar.asset_classification.morningstar_sector_code.latest
    profitable = morningstar.valuation_ratios.ev_to_ebitda.latest > 0 
    volume = USEquityPricing.volume
    pricing = USEquityPricing.close.latest    
    universe = ( Q500US()&(pricing>0)&(volume>1000000)&profitable&~sector.eq(103)&~sector.eq(104) )
    return Pipeline(screen=universe)

def before_trading_start(context, data):
    context.output = pipeline_output('my_pipeline')
    context.buy = []
    context.sell = []
    context.ETN_buy = []
    context.B = []
    context.S = []
    positions = []
    context.longs = []
    for sec in context.portfolio.positions:
        positions.append(sec)
        
    if len(positions) >= 1:
        context.AmountHeldData.append(len(positions))
        
    #print('=============================================================================================================' + '\n' )
    
            
def ANALYZE(context, data):
    Universe500=context.output.index.tolist()
    prices = data.history(Universe500,'price',3,'1d')
    daily_rets=np.log(prices/prices.shift(1))
    rets=(prices.iloc[-2] - prices.iloc[0]) / prices.iloc[0]
    # If you don't want to skip the most recent return, however, use .iloc[-1] instead of .iloc[-2]:
    # rets=(prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0]
    stdevs=daily_rets.std(axis=0)
    rets_df=pd.DataFrame(rets,columns=['five_day_ret'])
    stdevs_df=pd.DataFrame(stdevs,columns=['stdev_ret'])
    context.output=context.output.join(rets_df,how='outer')
    context.output=context.output.join(stdevs_df,how='outer')    
    context.output['ret_quantile']=pd.qcut(context.output['five_day_ret'],context.nq,labels=False)+1
    context.output['stdev_quantile']=pd.qcut(context.output['stdev_ret'],3,labels=False)+1
    context.longs=context.output[(context.output['ret_quantile']==1) & 
                                (context.output['stdev_quantile']<context.nq_vol)].index.tolist()
    context.shorts=context.output[(context.output['ret_quantile']==context.nq) & 
                                 (context.output['stdev_quantile']<context.nq_vol)].index.tolist()    
#============================================================================================
#REBALANCE
#============================================================================================
    Universe500=context.output.index.tolist()
    context.existing_longs=0
    context.existing_shorts=0
    #not in filtered universe and not a stock being controlled by the Volatility Algorithm  : sell 
    for security in context.portfolio.positions:
        if security not in Universe500 and security!=sid(23921) and security!=sid(39214) and security!=sid(40516) and security!=sid(38054) and security!=sid(21507) and security!=sid(21508) and security!=sid(38294) and security!=sid(39214):
            order_target_percent(security, 0)
        if security in Universe500:
            if data.can_trade(security):
                current_quantile=context.output['ret_quantile'].loc[security]
                if context.portfolio.positions[security].amount>0:
                    if (current_quantile==1) and (security not in context.longs):
                        context.existing_longs += 1
                    elif (current_quantile>1) and (security not in context.shorts):
                        order_target_percent(security, 0)
                elif context.portfolio.positions[security].amount<0:
                    if (current_quantile==context.nq) and (security not in context.shorts):
                        context.existing_shorts += 1
                    elif (current_quantile<context.nq) and (security not in context.longs):
                        order_target_percent(security, 0)
#============================================================================================
#ANALYZE
#============================================================================================
    spy = [sid(8554)]
    spxs = [sid(37083)]
    Velocity = 0
    #long_leverage = 0
    #short_leverage = 0
    for sec in spy:
        pri = data.history(spy, "price",200, "1d")
        pos_one = (pri[sec][-10:].mean())
        pos_two = (pri[sec][-165:].mean())
        
        if pos_one < pos_two:   
            context.LocalMaximize = False
            context.L = 0.5
            for security in context.longs:
                pri = data.history(context.longs, "price",200, "1d")
                #pos = 'pri[security]'
                pos_one = (pri[security][-1])
                pos_six = (pri[security][-2:].mean())
                #VELOCITY
                velocity_stop = (pos_one - pos_six)
                Velocity = velocity_stop
                if Velocity < 0:
                    context.longs.remove(security)
        else:
            context.LocalMaximize = True
            context.L = 1.0
            for security in context.longs:
                pri = data.history(context.longs, "price",200, "1d")
                #pos = 'pri[security]'
                pos_one = (pri[security][-1])
                pos_six = (pri[security][-2:].mean())
                #VELOCITY
                velocity_stop = (pos_one - pos_six)
                Velocity = velocity_stop
                if Velocity > 0:
                    context.longs.remove(security)
    for sec in spy:
        pri = data.history(spy, "price",200, "1d")
        pos_one = (pri[sec][-1])
        pos_six = (pri[sec][-45:].mean())
        Velocity = (pos_one - pos_six)
        
        if Velocity > -0.5:
            #long_leverage = 1.75
            pos_one = (pri[sec][-1])
            pos_six = (pri[sec][-2:].mean())
            #VELOCITY
            velocity_stop = (pos_one - pos_six)
            Velocity = velocity_stop
            if Velocity < -0.5:
                context.longs = []
            if Velocity < -1.0:
                context.longs = []
                for sec in spxs:
                    if data.can_trade(sec): 
                        if sec not in context.longs:
                            context.longs.append(sec)
            elif data.can_trade(sid(37083)):
                for sec in spxs:
                    context.sell.append(sec)
                
        else:
            pos_one = (pri[sec][-1])
            pos_six = (pri[sec][-2:].mean())
            Velocity = (pos_one - pos_six)
            if Velocity < -1.0:
                context.longs = []
                for sec in spxs:
                    if data.can_trade(sec) and sec not in context.longs:
                        context.longs.append(sec)
            else:
                for sec in spxs:
                    if data.can_trade(sec):
                        context.sell.append(sec)
    VolatilityUniverse = [ sid(39214),sid(38294),sid(21508),sid(21507),sid(38054),sid(40516),sid(23921),sid(39214)]        
    for sec in context.portfolio.positions:
        if sec not in VolatilityUniverse:
            if sec not in context.buy:
                context.sell.append(sec)
           
def sell (context, data):
    for sec in context.sell:
        if data.can_trade(sec):
            if sec not in context.B:
                order_target_percent(sec, 0)
                context.S.append(sec)            
        
def buy (context, data):
    for sec in context.longs:
        if data.can_trade(sec):
            if sec not in context.portfolio.positions:
                if sec not in context.S:
                    if(len(context.longs) > 0):
                        Portfolio_Value_With_Gold = context.portfolio.portfolio_value + context.Gold  
                        #Place an order to adjust a position to a target value | Total will be 40% of buying power.
                        order_target_value(sec, (0.40*Portfolio_Value_With_Gold)/len(context.longs) ) 
            
def GLOBAL_Maximize (context, data):
    inVol = []
    NOTinVol = []
    VolatilityUniverse = [ sid(39214),sid(38294),sid(21508),sid(21507),sid(38054),sid(40516),sid(23921),sid(39214)]
    for sec in context.portfolio.positions:
        if sec in VolatilityUniverse: 
            inVol.append(sec)
        else:
            NOTinVol.append(sec)
    for sec in context.portfolio.positions:
        if sec not in VolatilityUniverse and len(NOTinVol) > 0:
            Portfolio_Value_With_Gold = context.portfolio.portfolio_value + context.Gold  
            order_target_value(sec, 0.5*Portfolio_Value_With_Gold/len(NOTinVol) )
    for sec in context.portfolio.positions:
        if sec in VolatilityUniverse:
            Portfolio_Value_With_Gold = context.portfolio.portfolio_value + context.Gold  
            order_target_value(sec, 0.5*Portfolio_Value_With_Gold/len(inVol) )
        
def Local_Maximize (context, data):
    VolatilityUniverse = [ sid(39214),sid(38294),sid(21508),sid(21507),sid(38054),sid(40516),sid(23921),sid(39214)]
    for sec in context.portfolio.positions:
        if sec not in VolatilityUniverse:
            if sec not in context.B:
                if context.portfolio.positions[sec].cost_basis * 0.935 > context.portfolio.positions[sec].last_sale_price:
                    order_target_percent(sec, 0)
             
    for sec in context.portfolio.positions:
        if context.portfolio.positions[sec].amount < 0:
            order_target_percent(sec, 0)
            
def day_end (context, data):
    context.DayCounter += 1
    ReturnRate = ((context.portfolio.returns*100)/context.DayCounter)
    SecCount = 0
    for sec in context.portfolio.positions:
        SecCount += 1
    print('| END OF DAY SUMMARY |')
    print('Day:                            ' + str(context.DayCounter))
    print('Initial Portfolio Value:        $' + str(context.portfolio.starting_cash))
    print('Current Portfolio Value:        $' + str(context.portfolio.portfolio_value))
    print('Profit & Loss:                  $' + str(context.portfolio.pnl))
    print('Total Returns:                  ' + str(context.portfolio.returns * 100) + '%')
    print(' Daily Return:                  ' + str(ReturnRate) + '%')
    print('Leverage:                       ' + str(context.account.leverage * 100) + '%')
    print('Positions:                      ' + str(SecCount))
    print('Initial Margin Requirement:     ' + str(context.account.initial_margin_requirement))
    print('Maintenance Margin Requirement: ' + str(context.account.maintenance_margin_requirement))
 
def handle_data (context, data):
    Portfolio_Value_With_Gold = context.portfolio.portfolio_value + context.Gold
    
    record(Portfolio_Value_With_Gold_THOU = Portfolio_Value_With_Gold/1000.00)
    record(Positions_Value_THOU = context.portfolio.positions_value/1000.00)
    record(Net_Value_THOU = (Portfolio_Value_With_Gold - context.Gold)/1000.00)    
    record(Positions_TEN = float(len(context.portfolio.positions))/10.00)
    record(Leverage = context.account.leverage)
