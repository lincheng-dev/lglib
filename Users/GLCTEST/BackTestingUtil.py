#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pandas
import bisect
import datetime, time
from pandas.tseries.offsets import BDay
from FundInstrument import ABMergePos, FundSplitPos

def loadFundInfo(fundInfoFile):
    df         = pandas.read_csv(fundInfoFile, sep=',')
    baseTicker = df['baseTicker'].map(lambda x: str(x))
    aTicker    = df['aTicker'].map(lambda x: str(x))
    bTicker    = df['bTicker'].map(lambda x: str(x))
    aRatio     = df['abWeight'].map(lambda x: float(x.split(':')[0]))
    bRatio     = df['abWeight'].map(lambda x: float(x.split(':')[1]))
    upFold     = df['upFold']
    downFold   = df['downFold']
    sumab      = aRatio + bRatio
    aRatio     = aRatio/sumab
    bRatio     = bRatio/sumab
    newdf      = pandas.concat([baseTicker, aTicker, bTicker, aRatio, bRatio, upFold, downFold], axis=1)
    newdf.columns = ['Ticker', 'ATicker', 'BTicker', 'AWeight', 'BWeight', 'UpFold', 'DownFold']
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
    
    sv   = pandas.Series()
    sp   = pandas.Series()
    svol = pandas.Series()
    if ticker.startswith('16'):
        sv   = valdf['UnitValue'][valsttid:valendid]
        sp   = pxdf['UnitValue'][pxsttid:pxendid]
        svol = dummyS
    else:
        sv   = valdf['UnitValue'][valsttid:valendid]
        sp   = pxdf['Close'][pxsttid:pxendid]
        svol = pxdf['Amount'][pxsttid:pxendid]  
    if len(sv) == 0:
        sv = dummyS
    if len(sp) == 0:
        sp = dummyS
    if len(svol) == 0:
        svol = dummyS
    return (sv, sp, svol)

def loadFundData(fundDataFolder, fundInfo, simStart, simEnd):
    seriesList = []
    keyList    = []
    headerList = ['FundValue', 'AValue', 'BValue', 'FundPrice', 'APrice', 'BPrice', 'FundAmount', 'AAmount', 'BAmount']
    
    for ticker in fundInfo.index:
        aticker       = fundInfo.ix[ticker]['ATicker']
        bticker       = fundInfo.ix[ticker]['BTicker']
        fv, fp, fvol  = getFundTickerData(ticker, fundDataFolder, simStart, simEnd)
        av, ap, avol  = getFundTickerData(aticker, fundDataFolder, simStart, simEnd)
        bv, bp, bvol  = getFundTickerData(bticker, fundDataFolder, simStart, simEnd)
        sloc          = [fv, av, bv, fp, ap, bp, fvol, avol, bvol]
        dfloc         = pandas.concat(sloc, axis=1)
        dfloc.columns = headerList
        seriesList.append(dfloc)
        keyList.append(str(ticker))
        #print "Done "+ticker
    df = pandas.concat(seriesList, axis=1, keys=keyList)
    df.fillna(0., inplace=True)
    foutname = fundDataFolder+'/dumpdata_'+simStart.strftime('%Y%m%d')+'_'+simEnd.strftime('%Y%m%d')+'.csv'
    df.to_csv(foutname, sep=',')
    return df

def collectArbOp(fundInfo, pxSlice, arbMargin):
    arbList = []
    for ticker, px in pxSlice.groupby(level=0):
        tickerInfo  = fundInfo.ix[ticker]
        tickerpx    = px[ticker]
        mergeMargin = ABMergePos.getArbMargin(ticker, tickerpx, tickerInfo)
        splitMargin = FundSplitPos.getArbMargin(ticker, tickerpx, tickerInfo)
        if mergeMargin > arbMargin:
            infoDict = dict(tickerpx)
            infoDict.update(dict(tickerInfo))
            arbList.append((mergeMargin, 'MERGE', infoDict))
        if splitMargin > arbMargin:
            infoDict = dict(tickerpx)
            infoDict.update(dict(tickerInfo))
            arbList.append((splitMargin, 'SPLIT', infoDict))
    arbList.sort(reverse=True, key=lambda x: x[0])
    return arbList

def printDictList(fileName, dictList):
    if len(dictList) == 0:
        return
    fout    = open(fileName, 'w')
    keylist = sorted(dictList[0].keys())
    fout.write(';'.join(keylist)+';\n')
    for curDict in dictList:
        for key in keylist:
            fout.write(str(curDict[key])+';')
        fout.write('\n')
    fout.close()
    
if __name__ == "__main__":
    try:
        fundInfoFile   = sys.argv[1]
        fundDataFolder = sys.argv[2]
        simStartStr    = sys.argv[3]
        simEndStr      = sys.argv[4]
        outPath        = sys.argv[5]
    except:
        print "Usage: CMD fundInfoFile fundDataFolder simStart simEnd outPath"
        sys.exit(0)
    
    print "Collecting fund info"
    fundInfo = loadFundInfo(fundInfoFile)
    simStart = datetime.datetime(*(time.strptime(simStartStr.strip(), "%Y%m%d")[0:6]))
    simEnd   = datetime.datetime(*(time.strptime(simEndStr.strip(), "%Y%m%d")[0:6]))
    print "Collecting fund data"
    fundData = loadFundData(fundDataFolder, fundInfo, simStart, simEnd)
    
    cashAmt               = 200000.         # initial cash
    cashUnit              = 50000.          # least invest 50000. cash
    canInvestLessThanUnit = False           # can/cannot invest less than cash unit
    nbOpCheck             = 5               # number of oppurtunities to check
    arbMargin             = 1.e-8           # margin weight for doing trade
    curDate               = simStart
    curCashAmt            = cashAmt
    curMTM                = 0.0
    posList               = []
    mtmList               = []
    print "Start simulation"
    while curDate <= simEnd:
        print "Working on %s" % curDate.strftime("%Y%m%d")
        if curDate not in fundData.index:
            curDate = curDate + datetime.timedelta(1)
            continue
        pxSlice = fundData.ix[curDate]
        
        curMTM = 0.0
        for pos in posList:
            posMTM, posCash = pos.evolve(curDate, dict(pxSlice[pos.getTicker()]))
            curMTM         += posMTM
            curCashAmt     += posCash
        
        curMTM += curCashAmt
        mtmList.append({'Date': curDate.strftime("%Y%m%d"), 'MTM': str(curMTM), 'Cash': str(curCashAmt)})
        
        arbList     = collectArbOp(fundInfo, pxSlice, arbMargin)
        curCashUnit = max([curCashAmt / nbOpCheck, cashUnit])
        for arb in arbList[0:nbOpCheck]:
            # at least invest cashUnit
            if curCashAmt > cashUnit:
                infoDict              = arb[2]
                infoDict['StartDate'] = curDate
                newPos                = ABMergePos(**infoDict) if arb[1] == 'MERGE' else FundSplitPos(**infoDict)
                newCost               = newPos.setupAllocAmount(min([curCashAmt, curCashUnit]))
                curCashAmt           -= newCost
                posList.append(newPos)
                print "%s position created with %12.2f cash, remaining %12.2f" % (arb[1], newCost, curCashAmt)
        
        curDate = curDate + datetime.timedelta(1)
    
    fMTM    = outPath + '/MTM_log_' + simStartStr + '_' + simEndStr + '.log'
    fPos    = outPath + '/POS_log_' + simStartStr + '_' + simEndStr + '.log'
    sumList = [pos.getSummary() for pos in posList]
    printDictList(fMTM, mtmList)
    printDictList(fPos, sumList)
    
    
        