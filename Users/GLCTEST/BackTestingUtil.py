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
    newdf.columns = ['baseTicker', 'aTicker', 'bTicker', 'aWeight', 'bWeight']
    newdf.set_index(['baseTicker'], inplace=True)
    return newdf

def getFundTickerData(ticker, fundDataFolder, simStart, simEnd):
    fileName = fundDataFolder + '/' + ticker + '_PRICE.csv'
    if ticker.startswith('16'):
        fileName = fundDataFolder + '/' + ticker + '_VALUE.csv'
    dffull = pandas.read_csv(fileName, sep=',')
    dffull.set_index('Date', inplace=True)
    dffull = dffull.reindex(index=dffull.index[::-1]) # for descending data, ascending should be better
    dffull.index = dffull.index.map(lambda x: datetime.datetime(*(time.strptime(x, "(datetime.datetime(%Y, %m, %d, 0, 0),)")[0:6])))
    sttid  = bisect.bisect_left(dffull.index, simStart)
    endid  = bisect.bisect_left(dffull.index, simEnd)
    if ticker.startswith('16'):
        return dffull['UnitValue'][sttid:endid]
    else:
        return dffull['Close'][sttid:endid]
    
def loadFundData(fundDataFolder, fundInfo, simStart, simEnd):
    seriesList = []
    headerList = []
    for ticker in fundInfo.index:
        aticker = fundInfo.ix[ticker]['aTicker']
        bticker = fundInfo.ix[ticker]['bTicker']
        tseries = getFundTickerData(ticker, fundDataFolder, simStart, simEnd)
        aseries = getFundTickerData(aticker, fundDataFolder, simStart, simEnd)
        bseries = getFundTickerData(bticker, fundDataFolder, simStart, simEnd)
        if len(tseries) == 0:
            tseries = pandas.Series([0., 0.], index=[simStart, simEnd])
        if len(aseries) == 0:
            aseries = pandas.Series([0., 0.], index=[simStart, simEnd])
        if len(bseries) == 0:
            bseries = pandas.Series([0., 0.], index=[simStart, simEnd])
        seriesList += [tseries, aseries, bseries]
        headerList += [ticker, aticker, bticker]
        print "Done "+ticker
    df = pandas.concat(seriesList, axis=1)
    df.columns = headerList
    df.fillna(0., inplace=True)
    foutname = fundDataFolder+'/dumpdata_'+simStart.strftime('%Y%m%d')+'_'+simEnd.strftime('%Y%m%d')+'.csv'
    df.to_csv(foutname, sep=',')
    return df




class CashPos():

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
    
    
    
    
        