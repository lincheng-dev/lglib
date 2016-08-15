#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pandas
import bisect
import datetime, time
from pandas.tseries.offsets import BDay
import TransactionFee


def loadFundInfo(fundInfoFile):
    df         = pandas.read_csv(fundInfoFile, sep=',')
    baseTicker = df['baseTicker'].map(lambda x: str(x))
    aTicker    = df['aTicker'].map(lambda x: str(x))
    bTicker    = df['bTicker'].map(lambda x: str(x))
    aRatio     = df['abWeight'].map(lambda x: float(x.split(':')[0]))
    bRatio     = df['abWeight'].map(lambda x: float(x.split(':')[1]))
    sumab      = aRatio + bRatio
    aRatio     = aRatio/sumab
    bRatio     = bRatio/sumab
    newdf      = pandas.concat([baseTicker, aTicker, bTicker, aRatio, bRatio], axis=1)
    newdf.columns = ['Ticker', 'ATicker', 'BTicker', 'AWeight', 'BWeight']
    newdf.index   = newdf['Ticker']
    return newdf

def getFundTickerData(ticker, fundDataFolder, simStart, simEnd):
    dummyS  = pandas.Series([0., 0.], index=[simStart, simEnd])
    pxFile  = fundDataFolder + '/' + ticker + '_PRICE.csv'
    valFile = fundDataFolder + '/' + ticker + '_VALUE.csv'
    
    pxdf       = pandas.read_csv(pxFile, sep=',')
    pxdf.set_index('Date', inplace=True)
    #pxdf       = pxdf.reindex(index=pxdf.index[::-1]) # for descending data, ascending should be better
    pxdf.index = pxdf.index.map(lambda x: datetime.datetime(*(time.strptime(x, "(datetime.datetime(%Y, %m, %d, 0, 0),)")[0:6])))
    pxsttid    = bisect.bisect_left(pxdf.index, simStart)
    pxendid    = bisect.bisect_left(pxdf.index, simEnd)

    valdf       = pandas.read_csv(valFile, sep=',')
    valdf.set_index('Date', inplace=True)
    #valdf       = valdf.reindex(index=valdf.index[::-1]) # for descending data, ascending should be better
    valdf.index = valdf.index.map(lambda x: datetime.datetime(*(time.strptime(x, "(datetime.datetime(%Y, %m, %d, 0, 0),)")[0:6])))
    valsttid    = bisect.bisect_left(valdf.index, simStart)
    valendid    = bisect.bisect_left(valdf.index, simEnd)
    
    sv = pandas.Series()
    sp = pandas.Series()
    if ticker.startswith('16'):
        sv = valdf['UnitValue'][valsttid:valendid]
        sp = pxdf['UnitValue'][pxsttid:pxendid]
    else:
        sv = valdf['UnitValue'][valsttid:valendid]
        sp = pxdf['Close'][pxsttid:pxendid]
    if len(sv) == 0:
        sv = dummyS
    if len(sp) == 0:
        sp = dummyS
    return (sv, sp)

def loadFundData(fundDataFolder, fundInfo, simStart, simEnd):
    seriesList = []
    keyList    = []
    headerList = ['FundValue', 'AValue', 'BValue', 'FundPrice', 'APrice', 'BPrice']
    
    for ticker in fundInfo.index:
        aticker = fundInfo.ix[ticker]['ATicker']
        bticker = fundInfo.ix[ticker]['BTicker']
        fv, fp  = getFundTickerData(ticker, fundDataFolder, simStart, simEnd)
        av, ap  = getFundTickerData(aticker, fundDataFolder, simStart, simEnd)
        bv, bp  = getFundTickerData(bticker, fundDataFolder, simStart, simEnd)
        sloc    = [fv, av, bv, fp, ap, bp]
        dfloc   = pandas.concat(sloc, axis=1)
        dfloc.columns = headerList
        seriesList.append(dfloc)
        keyList.append(ticker)
        print "Done "+ticker
    df = pandas.concat(seriesList, axis=1, keys=keyList)
    df.fillna(0., inplace=True)
    foutname = fundDataFolder+'/dumpdata_'+simStart.strftime('%Y%m%d')+'_'+simEnd.strftime('%Y%m%d')+'.csv'
    df.to_csv(foutname, sep=',')
    return df

class CashPos():
    pass

if __name__ == "__main__":
    try:
        fundInfoFile   = sys.argv[1]
        fundDataFolder = sys.argv[2]
        simStartStr    = sys.argv[3]
        simEndStr      = sys.argv[4]
    except:
        print "Usage: CMD fundInfoFile fundDataFolder simStart simEnd"
        sys.exit(0)
    
    fundInfo = loadFundInfo(fundInfoFile)
    simStart = datetime.datetime(*(time.strptime(simStartStr.strip(), "%Y%m%d")[0:6]))
    simEnd   = datetime.datetime(*(time.strptime(simEndStr.strip(), "%Y%m%d")[0:6]))
    fundData = loadFundData(fundDataFolder, fundInfo, simStart, simEnd)
    
    
    
    
        