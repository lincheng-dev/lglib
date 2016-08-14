#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import TransactionFee
from numpy import floor

def NotExchFund(ticker):
    return ticker.startswith('16')

class ABFundPos(object):
    
    def __init__(self, *args, **kwargs):
        # basic info
        self.ticker             = {}
        self.ticker['Ticker']   = kwargs['Ticker']
        self.ticker['ATicker']  = kwargs['ATicker']
        self.ticker['BTicker']  = kwargs['BTicker']
        self.weight             = {}
        self.weight['AWeight']  = kwargs['AWeight']
        self.weight['BWeight']  = kwargs['BWeight']
        # initial px info
        self.sttpx              = {}
        self.sttpx['FundPrice'] = kwargs['FundPrice']
        self.sttpx['APrice']    = kwargs['APrice']
        self.sttpx['BPrice']    = kwargs['BPrice']
        self.sttpx['FundValue'] = kwargs['FundValue']
        self.sttpx['AValue']    = kwargs['AValue']
        self.sttpx['BValue']    = kwargs['BValue']
        # px status
        self.lastpx             = dict(self.sttpx)
        self.startDate          = kwargs['StartDate']
        self.endDate            = datetime.date(9999, 12, 31)
        self.lastDate           = self.startDate
        self.minUnwindDate      = (self.startDate + BDay(3)).to_datetime()
        # flags
        self.unwinded           = False
        self.upfolded           = False
        self.downfolded         = False
        self.isAlloced          = False
        self.unitshare          = 1000
        self.notExchTraded      = NotExchFund(self.ticker['Ticker'])
        
    def evolve(self, latestpx):
        raise NotImplementedError
        
    def setupAllocAmount(self, cashAmount):
        raise NotImplementedError   
        
    @staticmethod
    def getArbMargin(Ticker):
        raise NotImplementedError

    def validate(self, px):
        # always need BValue to check whether it is in normal status
        validvalue = px['AValue'] > 0.01 and px['BValue'] > 0.01 and px['FundValue'] > 0.01
        validprice = px['APrice'] > 0.01 and px['BPrice'] > 0.01 and px['FundPrice'] > 0.01
        return validvalue and validprice

    def getStartDate(self):
        raise self.startDate
    
    def isLive(self):
        return not self.unwinded
    
    def getEndDate(self):
        return self.endDate
    
    def getFundPos(self):
        return (self.APos, self.BPos, self.FPos)
    
    def getSummary(self):
        outdict   = {}
        outdict.update(self.ticker)
        outdict.update(self.sttpx)
        outdict.update(self.sttPos)
        for key in self.lastpx:
            outdict['last'+key] = self.lastpx[key]
        for key in self.lastPos:
            outdict['last'+key] = self.lastPos[key]
            
        outdict['startDate']  = self.startDate
        outdict['endDate']    = self.endDate
        outdict['upfolded']   = self.upfolded
        outdict['downfolded'] = self.downfolded
        outdict['totalPnL']   = self.totalpnl
        outdict['estPnL']     = self.estpnl
        outdict['mktPnL']     = self.mktpnl
        outdict['sttCost']    = self.sttCost
        outdict['lastMTM']    = self.lastMTM
        
        return outdict  
        
class ABMergePos(ABFundPos):
    
    def __init__(self, *args, **kwargs):
        # basic fact
        super(ABMergePos, self).__init__(*args, **kwargs)
    
    @staticmethod
    def getArbMargin(Ticker):
        # not exchange trade, buy AB at rate, sell with redemption
        if NotExchFund(Ticker):
            buyFee  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            sellFee = TransactionFee.TxFeeFundRedemp().getTxFeeRate()
            return buyFee + sellFee
        else:
            buyFee  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            sellFee = buyFee
            return buyFee + sellFee
    
    def setupAllocAmount(self, cashAmount):
        if self.notExchTraded:
            self.buyFeeRate  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.sellFeeRate = TransactionFee.TxFeeFundRedemp().getTxFeeRate()
        else:
            self.buyFeeRate  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.sellFeeRate = self.buyFeeRate            
        
        unitapx        = self.unitshare * self.weight['AWeight'] * self.sttpx['APrice']
        unitbpx        = self.unitshare * self.weight['BWeight'] * self.sttpx['BPrice']
        unitpx         = unitapx + unitbpx
        unitpxwfee     = unitpx * (1 + self.buyFeeRate)
        nbunits        = floor(cashAmount / unitpxwfee)
        self.sttCost   = unitpxwfee * nbunits
        self.sttFee    = (unitpxwfee - unitpx) * nbunits
        nbshare        = nbunits * self.unitshare
        self.sttPos    = {'Fund': 0, 'A': self.weight['AWeight'] * nbshare, 'B': self.weight['BWeight'] * nbshare}
        self.lastPos   = dict(self.sttPos)
        self.lastMTM   = (self.sttCost - self.sttFee) * (1 - self.sellFeeRate)
        self.isAlloced = True
        # return the cash flow
        return self.sttCost

    def evolve(self, date, px):
        if not self.isAlloced:
            raise "No cash alloced, need to call setupAllocAmount to init"
        if date < self.lastDate:
            raise "Should not evolve to past date %s while evolve date is %s" % (date.strftime("%Y%m%d"), self.lastDate.strftime("%Y%m%d"))
        if self.unwinded:
            return (0., 0.) # only return zero MTM/Cashflow, do nothing
        if self.validate(px):
            if px['BValue'] - self.lastpx['BValue'] > 0.2:
                # downfold happened
                if self.downfolded or self.upfolded:
                    raise "ticker %s, pos stt %s, date %s, multiple fold happen" % ('/'.join(self.Ticker), self.startDate.strftime("%Y%m%d"), date.strftime("%Y%m%d")) 
                self.downfolded = True
                ashare          = self.sttPos['A']
                bshare          = self.sttPos['B']
                fshare          = ashare * abs(self.lastpx['AValue'] - self.lastpx['BValue'])
                self.lastPos = {'Fund': fshare, 'A': ashare * self.lastpx['BValue'], 'B': bshare * self.lastpx['BValue']}
            if px['BValue'] - self.lastpx['BValue'] < -0.2:
                # upfold happened
                if self.downfolded or self.upfolded:
                    raise "ticker %s, pos stt %s, date %s, multiple fold happen" % ('/'.join(self.Ticker), self.startDate.strftime("%Y%m%d"), date.strftime("%Y%m%d")) 
                self.upfolded   = True
                ashare          = self.sttPos['A']
                bshare          = self.sttPos['B']
                fshare          = ashare * self.lastpx['AValue'] + bshare * self.lastpx['BValue'] - ashare - bshare
                self.lastPos = {'Fund': fshare, 'A': ashare, 'B': bshare}
            
            self.lastpx   = dict(px)
            self.lastDate = date
            if date >= self.minUnwindDate:
                self.unwinded = True
                self.endDate  = date
                fval          = px['FundValue'] if self.notExchTraded else px['FundPrice']
                self.lastMTM  = (self.lastPos['Fund'] + self.lastPos['A'] + self.lastPos['B']) * fval * (1 - self.sellFeeRate)
                return (0., self.lastMTM)
            else:
                fval          = px['FundValue'] if self.notExchTraded else px['FundPrice']
                self.lastMTM  = self.lastPos['Fund'] * fval * (1 - self.sellFeeRate) + (self.lastPos['A'] * px['APrice'] + self.lastPos['B'] * px['BPrice']) * (1 - self.buyFeeRate)
                return (self.lastMTM, 0.)  
        else:
            return (self.lastMTM, 0.)
            
    def getSummary(self):
        self.totalpnl  = self.lastMTM - self.sttCost
        fval           = self.sttpx['FundValue'] if self.notExchTraded else self.sttpx['FundPrice']
        self.estpnl    = (self.sttPos['A'] + self.sttPos['B']) * fval * (1 - self.sellFeeRate)
        self.mktpnl    = self.totalpnl - self.estpnl
        return super(ABMergePos, self).getSummary()
        
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