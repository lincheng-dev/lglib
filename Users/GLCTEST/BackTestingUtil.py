#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pandas
import bisect
import datetime, time
from pandas.tseries.offsets import BDay

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

STANDARD_FEE    = 0.005
FUND_TXFEE_DICT = {
'FUNDBUY'  : STANDARD_FEE, 
'FUNDSELL' : STANDARD_FEE, 
'ABUY'     : STANDARD_FEE, 
'ASELL'    : STANDARD_FEE, 
'BBUY'     : STANDARD_FEE, 
'BSELL'    : STANDARD_FEE, 
'SPLIT'    : 0., 
'MERGE'    : 0.}

class ABMergePos():
    
    def __init__(self, ticker, date, cashalloc, aweight, bweight, px, apx, bpx, feedict=FUND_TXFEE_DICT):
        # basic fact
        self.px      = px 
        self.ticker  = ticker
        self.apx     = apx
        self.bpx     = bpx
        self.aw      = aweight
        self.bw      = bweight
        self.fdict   = feedict
        self.sttdate = date
        self.lastpx  = px
        self.lastapx = apx
        self.lastbpx = bpx
        self.lastday = self.sttdate
        self.unwdate = (self.sttdate + BDay(3)).to_datetime()
        self.unwind  = False
        self.dfold   = False
        self.ufold   = False
        
        # number of contract, suppose we buy multiple of 1000
        unit          = 1000
        unitapx       = self.aw * unit * (1. + self.fdict['ABUY']) * self.apx
        unitbpx       = self.bw * unit * (1. + self.fdict['BBUY']) * self.bpx
        unitpx        = unitapx + unitbpx
        nblots        = floor(cashalloc / unitpx)
        self.buycash  = nblots * unitpx
        buycosta      = nblots * self.aw * unit * self.fdict['ABUY'] * self.apx
        buycostb      = nblots * self.bw * unit * self.fdict['BBUY'] * self.bpx
        self.buycost  = buycosta + buycostb
        self.buylots  = self.nblots
        self.nblots   = unit * nblots
        self.mcost    = self.nblots * self.fdict['MERGE']
        self.sellcost = self.nblots * self.px * self.fdict['FUNDSELL']
        self.sellcash = self.nblots * self.px * (1. - self.fdict['FUNDSELL']) - self.mcost
        self.estpnl   = self.sellcash - self.buycash
    
    def getTicker(self):
        return ticker
        
    def getSetupCost(self):
        return self.buycash
    
    def evolve(self, date, px, apx, bpx):
        if date < self.lastday:
            raise 'should not evolve to past date'
        if self.unwind:
            return 0. # only return zero MTM, do nothing
        if px > 0.01 and apx > 0.01 and bpx > 0.01:
            
            # check whether fold happens or not, amend nblots accordingly
            if bpx - self.lastbpx > 0.2:
                # suppose downfold happens
                self.dfold  = True
                # just an approximation coz price is different from value
                newlots     = self.lastbpx * self.nblots * self.bw + self.lastapx * self.nblots * self.aw
                newmcost    = self.lastbpx * self.mcost
                self.nblots = newlots
                self.mcost  = newmcost
            if bpx - self.lastbpx < -0.2:
                # suppose upflod happens
                self.ufold  = True
                # just an approximation coz price is different from value
                newlots     = self.lastbpx * self.nblots * self.bw + self.lastapx * self.nblots * self.aw
                self.nblots = newlots
                
            if date >= self.unwdate:
                self.unwind   = True # position finished no need to continue
                self.estscost = self.px * self.nblots * self.fdict['FUNDSELL']
                    
            if self.dfold and self.ufold:
                raise 'down fold and up fold both happen for %s at %s' %(str(self.ticker), date.strftime('%Y%m%d'))
            
            # prepare for MTM
            self.lastpx   = px
            self.lastapx  = apx
            self.lastbpx  = bpx
            self.lastday  = date
            
        self.sellcost = self.nblots * self.lastpx * self.fdict['FUNDSELL']
        self.sellcash = self.nblots * self.lastpx * (1. - self.fdict['FUNDSELL']) - self.mcost
        if self.unwind:
            # no MTM, with cash
            return (0., self.sellcash)
        else:
            # only MTM
            return (self.sellcash, 0.)
   
    def getSummary(self):
        totalpnl  = self.sellcash - self.buycash
        totalcost = self.buycost + self.mcost, self.sellcost
         
        return [self.ticker, self.sttdate, self.px, self.apx, self.bpx, 
                self.lastday, self.lastpx, self.lastapx, self.lastbpx, 
                self.buylots, self.nblots, self.dfold, self.ufold,
                self.buycost, self.mcost, self.sellcost, totalcost,
                totalpnl, estpnl]   
        
class FundSplitPos():
    def __init__(self, ticker, date, cashalloc, aweight, bweight, px, apx, bpx, feedict=FUND_TXFEE_DICT):
        # constant facts
        self.ticker   = ticker
        self.aw       = aweight
        self.bw       = bweight
        self.fdict    = feedict
        self.sttpx    = px
        self.sttapx   = apx
        self.sttbpx   = bpx
        self.sttdate  = date
        self.unwdate  = (date + BDay(3)).to_datetime()
        
        # update variables
        self.lastpx   = px
        self.lastapx  = apx
        self.lastbpx  = bpx
        self.lastdate = date
        self.unwind   = False
        self.dfold    = False
        self.ufold    = False
        
        # number of contract, suppose we buy multiple of 1000
        unit          = 1000
        unitpx        = unit * (1. + feedict['FUNDBUY']) * px
        nbunitlots    = floor(cashalloc / unitpx)
        self.nblots   = unit * nbunitlots
        self.buylots  = self.nblots
        self.buycash  = nbunitlots * unitpx
        self.buycost  = nbunitlots * unit * feedict['FUNDBUY'] * px
        self.splcost  = self.nblots * self.fdict['SPLIT']
        self.sellcost = self.nblots * self.px * self.fdict['FUNDSELL']
        self.sellcash = self.nblots * self.px * (1. - self.fdict['FUNDSELL']) - self.mcost
        self.estpnl   = self.sellcash - self.buycash
    
    def getTicker(self):
        return ticker
        
    def getSetupCost(self):
        return self.buycash
    
    def evolve(self, date, px, apx, bpx):
        if date < self.lastday:
            raise 'should not evolve to past date'
        if self.unwind:
            return 0. # only return zero MTM, do nothing
        if px > 0.01 and apx > 0.01 and bpx > 0.01:
            
            # check whether fold happens or not, amend nblots accordingly
            if bpx - self.lastbpx > 0.2:
                # suppose downfold happens
                self.dfold  = True
                # just an approximation coz price is different from value
                newlots     = self.lastbpx * self.nblots * self.bw + self.lastapx * self.nblots * self.aw
                newmcost    = self.lastbpx * self.mcost
                self.nblots = newlots
                self.mcost  = newmcost
            if bpx - self.lastbpx < -0.2:
                # suppose upflod happens
                self.ufold  = True
                # just an approximation coz price is different from value
                newlots     = self.lastbpx * self.nblots * self.bw + self.lastapx * self.nblots * self.aw
                self.nblots = newlots
                
            if date >= self.unwdate:
                self.unwind   = True # position finished no need to continue
                self.estscost = self.px * self.nblots * self.fdict['FUNDSELL']
                    
            if self.dfold and self.ufold:
                raise 'down fold and up fold both happen for %s at %s' %(str(self.ticker), date.strftime('%Y%m%d'))
            
            # prepare for MTM
            self.lastpx   = px
            self.lastapx  = apx
            self.lastbpx  = bpx
            self.lastday  = date
            
        self.sellcost = self.nblots * self.lastpx * self.fdict['FUNDSELL']
        self.sellcash = self.nblots * self.lastpx * (1. - self.fdict['FUNDSELL']) - self.mcost
        if self.unwind:
            # no MTM, with cash
            return (0., self.sellcash)
        else:
            # only MTM
            return (self.sellcash, 0.)
   
    def getSummary(self):
        totalpnl  = self.sellcash - self.buycash
        totalcost = self.buycost + self.mcost, self.sellcost
         
        return [self.ticker, self.sttdate, self.px, self.apx, self.bpx, 
                self.lastday, self.lastpx, self.lastapx, self.lastbpx, 
                self.buylots, self.nblots, self.dfold, self.ufold,
                self.buycost, self.mcost, self.sellcost, totalcost,
                totalpnl, estpnl]   

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
    
    
    
    
        