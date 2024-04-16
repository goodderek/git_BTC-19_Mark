import pymysql
import numpy as np
import pandas as pd
import time,os,ccxt
from datetime import datetime,timedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

import talib as ta
import requests




    


def handle_data(df,fast,slow,factor_out):
    # 把string改成float，以利後續計算：
    df['open']=df['price_open'].astype(float)
    df['high']=df['price_high'].astype(float)
    df['low']=df['price_low'].astype(float)
    df['close']=df['price_close'].astype(float)
    df['volume']=df['volume'].astype(float)

    #---------集中drop 區-----------------------------------------------------
    df['EMA_fast'] = ab.EMA(df,timeperiod=fast)
    df['EMA_slow'] = ab.EMA(df,timeperiod=slow)
    df['EMA_200'] = ab.EMA(df,timeperiod=200)
    # df.dropna(inplace=True) #集中
    df['W52_LL']=df['close'].rolling(365).min()
    # df.dropna(inplace=True) #集中
    df['W52_HH']=df['close'].rolling(365).max()
    # df.dropna(inplace=True) #集中

    df['ATR'] = ab.ATR(df,timeperiod=fast)
    df['HH']=df['close'].rolling(fast).max()
    df.dropna(inplace=True) #集中









    #---------condition1 --------------------------------------------------------------
    # df['EMA_fast'] = ab.EMA(df,timeperiod=fast)
    # df['EMA_slow'] = ab.EMA(df,timeperiod=slow)
    # df['EMA_200'] = ab.EMA(df,timeperiod=200)
    # df.dropna(inplace=True) #集中
    
    df['EMA_rank-1']=(df['close']-df['EMA_slow']).apply(lambda x:1 if x>0 else 0) #價格>150日
    df['EMA_rank-2']=(df['close']-df['EMA_200']).apply(lambda x:1 if x>0 else 0) #價格>200日
    df['EMA_rank-3']=(df['EMA_fast']-df['EMA_slow']).apply(lambda x:1 if x>0 else 0) #MA50>150日
    df['EMA_rank-4']=(df['EMA_fast']-df['EMA_200']).apply(lambda x:1 if x>0 else 0) #MA50>200日
 
    # df['EMA_rank']=(df['EMA_rank-1']+df['EMA_rank-2']+df['EMA_rank-3']+df['EMA_rank-4']).apply(lambda x:1 if x==4 else 0)  #以上4項都要符合
    df['EMA_rank']=(df['EMA_rank-1']+df['EMA_rank-2']).apply(lambda x:1 if x==2 else 0)  
    
    
    
    #---------condition2 --------------------------------------------------------------
    df['200_rank']=(df['close']-df['EMA_200']).apply(lambda x:1 if x>0 else 0)
    df['bull_cnt']=df['200_rank'].rolling(30).agg(lambda x: (x>0).sum())
    df.dropna(inplace=True) 
    df['bull_cnt_rank']=df['bull_cnt'].apply(lambda x:1 if x>=30 else 0) #連續30天都在MA200之上



    
    #---------condition3: --------------------------------------------------------------
    # df['W52_LL']=df['close'].rolling(365).min()
    # df.dropna(inplace=True) #集中

    df['W52_LL_rank']=(df['close']/df['W52_LL']).apply(lambda x:1 if x>1.25 else 0) #目前價格大於52周低點至少25%(最好是100%)



    #---------condition4: --------------------------------------------------------------
    # df['W52_HH']=df['close'].rolling(365).max()
    # df.dropna(inplace=True) #集中

    df['W52_HH_rank']=(df['close']/df['W52_HH']).apply(lambda x:1 if x>0.75 else 0) #目前價格距離52周高點不超過25%(愈接近新高愈好)





    #---------Chandelier------------------------------
    # df['ATR'] = ab.ATR(df,timeperiod=fast)
    # df['HH']=df['close'].rolling(fast).max()
    # df.dropna(inplace=True) #集中

    df['Chand_stop']=(df['close']-(df['HH']-factor_out*df['ATR'])).apply(lambda x:1 if x>0 else 0) #1 表示收盤價在吊燈上方，繼續持有
    # df['Chandelier_bull_rank_exit']=(df['close']-(df['HH']-0.9*factor_out*df['ATR'])).apply(lambda x:1 if x>0 else 0) #ATR*0.9-->早點離場




    #定義進出訊號：
    df['entries']=(df['EMA_rank']+df['bull_cnt_rank']+df['W52_LL_rank']+df['W52_HH_rank']+df['Chand_stop']).apply(lambda x:1 if x>=5  else 0) 
    # df['entries']=(df['EMA_rank']+df['bull_cnt_rank']+df['W52_LL_rank']+df['W52_HH_rank']).apply(lambda x:1 if x==4  else 0) 
    df['exits']=(df['Chand_stop']).apply(lambda x:1 if x==0 else 0) 

    


    #砍掉多餘的：
    # df.drop(columns=['human_time','open_time'],inplace=True) #把時間刪掉了
    df.dropna(inplace=True)
    # df=df.reset_index()



    return df




def send_telegram(my_message):
    
    TOKEN = '5138323032:AAF17vy8MM4wfgt_t2iUeTtVbtlOd47CQF4'
    CHAT_ID = '-792732654' #這個chat id是goodderek_coin_trade
    SEND_URL = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': my_message}) 



def send_twitter(my_message):
    
    import tweepy
    consumer_key='DpQaZJCKhcqTWkNvbcv8iFlgQ'
    consumer_secret='9ROAv8As34geDvSh8GyuMY5HDCPa3V94UcDReFjTes3hFzgqvH'
    access_token='2203133095-F0JmWtF00wefU8B3AvNYgODpSuvcc9xEcJfsE9E'
    access_token_secret='ra3XXPUN7ECIR7ljUgFaonvcpfw87HhKA6KQHRX631DAb'

    client = tweepy.Client(consumer_key= consumer_key,consumer_secret= consumer_secret,access_token= access_token,access_token_secret= access_token_secret)
    response = client.create_tweet(text=my_message)



def get_quantityPrecision(symbol):
    for x in info['symbols']:
        if x['symbol'] == symbol:
            return x['quantityPrecision']
        
def get_pricePrecision(symbol):
    for x in info['symbols']:
        if x['symbol'] == symbol:
            return x['pricePrecision']  
        

def profit_trailing(symbol,side):

    #先砍掉原先的stop loss單：
    try:
        open_order_ID=client.futures_get_open_orders(symbol=symbol.replace("/",""))[0]['orderId']
        client.futures_cancel_order(symbol=symbol.replace("/",""), orderId=open_order_ID)
    except:
        print('del original 1% SL order first -->but no open order. ')


    
    if side=='SELL':  #空單要低價
        side_close='BUY'
    elif side=='BUY':  #多單要高價
        side_close='SELL'

  

    while 1: 

        #get current price:
        url = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr?symbol='+symbol.replace('/',''))
        price = float(url.json()['lastPrice'])
        
        #check direction to set stopPrice:
        if side_close=='BUY':
            stopPrice=round(price*(1.00033),pricePrecision)
        else:
            stopPrice=round(price*(0.99966),pricePrecision)


        myorderID=client.futures_create_order(symbol=symbol.replace("/",""), side=side_close, type='STOP_MARKET', stopPrice=stopPrice, closePosition='true')['orderId'] 
        time.sleep(33) #等33秒

        #check active position:
        position=client.futures_account()['positions']
        df_temp=pd.DataFrame(position)
        active_position=float(df_temp[df_temp['symbol']==symbol.replace('/','')].notional)
        

        if active_position ==0 :

            #save result to CSV file(關單時): (日後用strategy name當篩選就好)
            df_result=pd.read_csv('./trading/'+symbol.replace('/','')+'.csv')
            df_result.iloc[len(df_result)-1,-3]=current_time
            df_result.iloc[len(df_result)-1,-2]=stopPrice

            if side=='SELL': #空單:前-後
                my_profit=round((df_result['open_price'].iloc[-1]-df_result['close_price'].iloc[-1])/df_result['open_price'].iloc[-1]*100,2)
                df_result.iloc[len(df_result)-1,-1]=my_profit
            else: #多單：後-前
                my_profit=round((df_result['close_price'].iloc[-1]-df_result['open_price'].iloc[-1])/df_result['open_price'].iloc[-1]*100,2)
                df_result.iloc[len(df_result)-1,-1]=my_profit

            df_result.to_csv('./trading/'+symbol.replace('/','')+'.csv',index=False)
            
            # profit=df_result.iloc[-1,-1]


            #send messege to telegram:
            my_message=f'[{strategy}-close] {side_close} , {symbol}@{stopPrice} ({my_profit}%)'
            send_telegram(my_message)
            # send_twitter(my_message)

            break

        else:
            #若沒成交，砍單再來一次
            client.futures_cancel_order(symbol=symbol.replace("/",""), orderId=myorderID)
            print(f'---price={price}-still open---> run again to close the position----------------')    
    




################################### main body #############################################

#-------------------------------------- API and setup --------------------------------------
from binance.client import Client
# api_key='hcEGmkf1aLxgKDsPvMscaFvo4QE9ddgN9H8e4Qi6cRDkK5QTbOoI3knLPFlAqwFn' #普通合約
# api_secret='fMofHhh5oAyDm2dbr4LegrukovVQDLqsaqwXqyH3rUYx3MWfkGaPIPHHz2iQP6iU' #有設定固定ip:125.228.161.82

api_key='bzNIJ7fTosTQiEctCUorHTibedNOECxpLL6A5t5FGp7zXY3CMKmkyIrF59cWLR0h' #跟單交易的
api_secret='wu5VaFM0EoomcJzqwuG7ROUB7gquNDEKprgMM5kHJUJzPly93zBEQ7o3pfJhfRUA' #有設定固定ip:125.228.161.82

client = Client(api_key=api_key, api_secret=api_secret,testnet = False)
info = client.futures_exchange_info() #get exchange information:






symbols=['FTM/USDT','SOL/USDT','DOT/USDT'] #BTC 有開單最小量限制，較不好拿來測 , shib好像有乘以1000先捨棄

for symbol in symbols:

    #--------------------------------------  general parameters -------------------------------------- 
    budget=9
    current_time=datetime.now().strftime("%Y/%m/%d_%H")
    strategy='mark-1' 
    leverage=9

    

    #--------------------------------------  indivisual parameters --------------------------------------
    #2023/12/13新增改自動每周update GA parameters: (用這個就不用if條件式一個一個key了)
    df_para=pd.read_csv('GA-19.csv')
    mask=(df_para['strategy']==strategy) & (df_para['symbol']==symbol) & (df_para['result']!=0.01)  

    fast=df_para[mask]['x'].iloc[-1]
    slow=df_para[mask]['y'].iloc[-1]
    factor_out=df_para[mask]['z'].iloc[-1] #2023/12/14改成由csv file除以10了，這裡直接調用不用再除

    print(f'-------{symbol}:fast={fast},slow={slow},factor_out={factor_out}---------')





    # ########################### 4h data (加卡4小時的DEMA200) ##########################
    # candle='4h'
    # start_time=datetime.now()-timedelta(days=163) #1h的candle-->1000/6=166days -->安全一點用163天
    
    # start_timestamp=int(datetime.timestamp(start_time))*1000 #ms要乘1000

    # #-------------------------------------- get data from binance: (這個是現貨的，先暫用，以後改成用合約的)--------------------------------------
    # binance=ccxt.binance()
    # data = binance.fetch_ohlcv(symbol=symbol, timeframe=candle, since=start_timestamp, limit=1000) #用2017/8/18幣安開始的時間1502985600000
    # df = pd.DataFrame(data)
    # df.rename(columns={0: 'open_time', 1: 'price_open', 2: 'price_high', 3: 'price_low', 4: 'price_close', 5: 'volume'}, inplace=True)
    # df['human_time'] = pd.to_datetime(df['open_time'],unit='ms')+pd.Timedelta('08:00:00')
    # df.drop(len(df)-1,inplace=True) #最後一筆仍未結算，不列入  



    # #-------------------------------------- handle data --------------------------------------
    # df=handle_data(df,fast,slow,factor_out)











    ########################### 15m -->1h (RSI-11開始改，2024/2/11) ##########################
    # candle='1h'
    # start_time=datetime.now()-timedelta(days=39) #1h的candle-->1000/24=41.6days -->安全一點用39天
    
    candle='15m'
    start_time=datetime.now()-timedelta(days=9) #15m的candle-->1000/24/4=10.4days -->安全一點用9天
    
    start_timestamp=int(datetime.timestamp(start_time))*1000 #ms要乘1000

    #-------------------------------------- get data from binance: (這個是現貨的，先暫用，以後改成用合約的)--------------------------------------
    binance=ccxt.binance()
    data = binance.fetch_ohlcv(symbol=symbol, timeframe=candle, since=start_timestamp, limit=1000) #用2017/8/18幣安開始的時間1502985600000
    df = pd.DataFrame(data)
    df.rename(columns={0: 'open_time', 1: 'price_open', 2: 'price_high', 3: 'price_low', 4: 'price_close', 5: 'volume'}, inplace=True)
    df['human_time'] = pd.to_datetime(df['open_time'],unit='ms')+pd.Timedelta('08:00:00')
    df.drop(len(df)-1,inplace=True) #最後一筆仍未結算，不列入  



    #-------------------------------------- handle data --------------------------------------
    df=handle_data(df,fast,slow,factor_out)


    #-----------------------------------------get last record --------------------------------------
    entries=df['entries'].iloc[-1]
    # entries_bear=df['entries_bear'].iloc[-1]
    exits=df['exits'].iloc[-1]
    # exits_bear=df['exits_bear'].iloc[-1]












    ########################### before trading-(get precision and position)---by symbol ##########################
    #get_pricePrecision:
    pricePrecision=get_pricePrecision(symbol.replace('/',''))
    #get current price:
    url = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr?symbol='+symbol.replace('/',''))
    price = float(url.json()['lastPrice'])
    #get_quantityPrecision:
    quantityPrecision=get_quantityPrecision(symbol.replace('/',''))
    quantity=round(budget / price,quantityPrecision) #quantity是那個幣的數量，所以要除以price，才會變成用budget買此合約

    #check active position:
    position=client.futures_account()['positions']
    df_temp=pd.DataFrame(position)
    active_position=float(df_temp[df_temp['symbol']==symbol.replace('/','')].notional)
   

    #change leverage: (https://stackoverflow.com/questions/67643077/how-can-i-adjust-the-leverage-with-bianance-api)
    client.futures_change_leverage(symbol=symbol.replace('/',''), leverage=leverage)





    #-------------------------------------- check csv file and auto update stop loss data:--------------------------------------
    df0=pd.read_csv('./trading/'+symbol.replace('/','')+'.csv')
    if active_position==0 and df0['close_time'].isna().sum()==1 : #無倉位，有1空白欄(na)
        df0.iloc[-1,5]='stop_loss'
        df0.iloc[-1,6]='stop_loss'
        df0.iloc[-1,7]='-2'

        #write it down:
        df0.to_csv('./trading/'+symbol.replace('/','')+'.csv',index=False)
        #update telegram, too.
        my_message=f'[{strategy}]: {symbol} STOP LOSS (-2%)  '
        send_telegram(my_message)
        # send_twitter(my_message)

    elif active_position!=0 and df0['close_time'].isna().sum()==2 : #有倉位，有2空白欄(na)
        df0.iloc[-2,5]='stop_loss'
        df0.iloc[-2,6]='stop_loss'
        df0.iloc[-2,7]='-2'

        #write it down:
        df0.to_csv('./trading/'+symbol.replace('/','')+'.csv',index=False)
        #update telegram, too.
        my_message=f'[{strategy}]: {symbol} STOP LOSS (-2%)  '
        send_telegram(my_message)
        # send_twitter(my_message)







    ########################### trading  ##########################
    if active_position==0: #空手，建立倉位
        
        # if entries_bear==1: #and DEMA_200_rank_4h==-1:  #bear market 
        
        #     side='SELL'

        #     #market order:
        #     client.futures_create_order(symbol=symbol.replace('/',''), side=side, type='MARKET', quantity=quantity) 
        #     #加開一個Stop loss (1%): (記得profit trailing那邊也要先close 此order)
        #     client.futures_create_order(symbol=symbol.replace("/",""), side='BUY', type='STOP_MARKET', stopPrice=round(price*1.02,pricePrecision), closePosition='true')     
            
        #     #send messege to telegram:
        #     my_message=f'[{strategy}-open] {side} , {symbol}@{price}'
        #     send_telegram(my_message)
        #     # send_twitter(my_message)

        #     #save result to CSV file:
        #     df_result=pd.read_csv('./trading/'+symbol.replace('/','')+'.csv')
        #     df_result.loc[len(df_result)]=[strategy,symbol,current_time,price,side,'','','']
        #     df_result.to_csv('./trading/'+symbol.replace('/','')+'.csv',index=False)


        if entries==1:  #and DEMA_200_rank_4h==1: #bull market

            # budget=7777 #雙重bull, 提高budget
            side='BUY'

            #market order:
            client.futures_create_order(symbol=symbol.replace('/',''), side=side, type='MARKET', quantity=quantity) 
            #加開一個Stop loss (1%):
            client.futures_create_order(symbol=symbol.replace("/",""), side='SELL', type='STOP_MARKET', stopPrice=round(price*0.98,pricePrecision), closePosition='true')    
            
            #send messege to telegram:
            my_message=f'[{strategy}-open] {side} , {symbol}@{price}'
            send_telegram(my_message)
            # send_twitter(my_message)
            
            time.sleep(6) #為何matic會無法更新至csv，先sleep 6秒試試(2024/2/23)

            #save result to CSV file:
            df_result=pd.read_csv('./trading/'+symbol.replace('/','')+'.csv')
            df_result.loc[len(df_result)]=[strategy,symbol,current_time,price,side,'','','']
            df_result.to_csv('./trading/'+symbol.replace('/','')+'.csv',index=False)
        
        else:
            print(f'{symbol}:no position and nothing to do.')



    else: #有倉位，關倉
        
        # strategy='Stack-close' 

        #get open price & side from csv file:
        df=pd.read_csv('./trading/'+symbol.replace('/','')+'.csv')
        side=df.iloc[-1,4] #side
        open_price=df.iloc[-1,3]
        if side=='SELL': #空單:前-後
            profit=round((open_price-price)/open_price,3)
        else: #多單：後-前
            profit=round((price-open_price)/price,3)
    





        if side=='BUY' and exits==1: #多單遇到空訊號，close position

            profit_trailing(symbol,side)

        

        # elif side=='SELL' and exits_bear==1: #空單遇到多訊號，close position

        #     profit_trailing(symbol,side)

        
        else:
            print(f'{symbol}:have position but nothing to do.')    







