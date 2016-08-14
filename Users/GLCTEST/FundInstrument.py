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
        self.evolveCounter      = 0
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

    @staticmethod
    def validate(px):
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
    def getArbMargin(Ticker, px, wt):
        if not ABFundPos.validate(px):
            return 0.0
            
        apx = px['APrice']
        bpx = px['BPrice']  
        buyFee  = 0.
        sellFee = 0. 
        fval    = 0.     
        if NotExchFund(Ticker):
            buyFee  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            sellFee = TransactionFee.TxFeeFundRedemp().getTxFeeRate()
            fval    = px['FundValue']
        else:
            buyFee  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            sellFee = buyFee
            fval    = px['FundPrice']
        margin = fval / (apx * wt['AWeight'] + bpx * wt['BWeight']) - 1. - buyFee - sellFee
        return max([margin, 0.0])
    
    def setupAllocAmount(self, cashAmount):
        if self.notExchTraded:
            self.buyFeeRate  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.sellFeeRate = TransactionFee.TxFeeFundRedemp().getTxFeeRate()
            self.unwindRate  = self.buyFeeRate 
        else:
            self.buyFeeRate  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.sellFeeRate = self.buyFeeRate            
            self.unwindRate  = self.buyFeeRate 
        
        unitapx        = self.unitshare * self.weight['AWeight'] * self.sttpx['APrice']
        unitbpx        = self.unitshare * self.weight['BWeight'] * self.sttpx['BPrice']
        unitpx         = unitapx + unitbpx
        unitpxwfee     = unitpx * (1 + self.buyFeeRate)
        nbunits        = floor(cashAmount / unitpxwfee)
        self.sttCost   = unitpxwfee * nbunits
        self.sttFee    = (unitpxwfee - unitpx) * nbunits
        nbshare        = nbunits * self.unitshare
        self.sttPos    = {'Fund': 0., 'A': self.weight['AWeight'] * nbshare, 'B': self.weight['BWeight'] * nbshare}
        self.lastPos   = dict(self.sttPos)
        self.lastMTM   = (self.sttCost - self.sttFee) * (1 - self.unwindRate)
        self.isAlloced = True
        # return the cash flow
        return self.sttCost

    def evolve(self, date, px):
        if not self.isAlloced:
            raise "No cash alloced, need to call setupAllocAmount to init"
        if date <= self.lastDate:
            raise "Should not evolve to past date %s while evolve date is %s" % (date.strftime("%Y%m%d"), self.lastDate.strftime("%Y%m%d"))
        if self.unwinded:
            return (0., 0.) # only return zero MTM/Cashflow, do nothing
        if self.validate(px):
            self.evolveCounter += 1
            # roll to base at T+1, suppose when we buy A and B at T, there is no change to up/down fold
            if self.evolveCounter == 1:
                self.lastPos = {'Fund': self.sttPos['A']+self.sttPos['B'], 'A': 0., 'B': 0.}
            
            self.lastpx   = dict(px)
            self.lastDate = date
            fval          = px['FundValue'] if self.notExchTraded else px['FundPrice']
            self.lastMTM  = self.lastPos['Fund'] * fval * (1 - self.sellFeeRate) + (self.lastPos['A'] * px['APrice'] + self.lastPos['B'] * px['BPrice']) * (1 - self.unwindRate)
            # T+2 we will be able to unwind the position
            if self.evolveCounter >= 2:
                self.unwinded = True
                self.endDate  = date
                return (0., self.lastMTM)
            else:
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
    
    def __init__(self, *args, **kwargs):
        # basic fact
        super(ABMergePos, self).__init__(*args, **kwargs)
    
    @staticmethod
    def getArbMargin(Ticker, px, wt):
        if not ABFundPos.validate(px):
            return 0.0
            
        apx = px['APrice']
        bpx = px['BPrice']  
        buyFee  = 0.
        sellFee = 0. 
        fval    = 0.     
        if NotExchFund(Ticker):
            buyFee  = TransactionFee.TxFeeFundSub().getTxFeeRate()
            sellFee = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            fval    = px['FundValue']
        else:
            buyFee  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            sellFee = buyFee
            fval    = px['FundPrice']
        margin = (apx * wt['AWeight'] + bpx * wt['BWeight']) / fval - 1. - buyFee - sellFee
        return max([margin, 0.0])
    
    def setupAllocAmount(self, cashAmount):
        unitpx = 0.0
        if self.notExchTraded:
            self.buyFeeRate  = TransactionFee.TxFeeFundSub().getTxFeeRate()
            self.sellFeeRate = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.unwindRate  = TransactionFee.TxFeeFundRedemp().getTxFeeRate()
            unitpx           = self.unitshare * self.sttpx['FundValue']
        else:
            self.buyFeeRate  = TransactionFee.TxFeeFundExchTrade().getTxFeeRate()
            self.sellFeeRate = self.buyFeeRate 
            self.unwindRate  = self.buyFeeRate 
            unitpx           = self.unitshare * self.sttpx['FundPrice']
        
        unitpxwfee     = unitpx * (1 + self.buyFeeRate)
        nbunits        = floor(cashAmount / unitpxwfee)
        self.sttCost   = unitpxwfee * nbunits
        self.sttFee    = (unitpxwfee - unitpx) * nbunits
        nbshare        = nbunits * self.unitshare
        self.sttPos    = {'Fund': nbshare, 'A': 0., 'B': 0.}
        self.lastPos   = dict(self.sttPos)
        self.lastMTM   = (self.sttCost - self.sttFee) * (1 - self.unwindRate)
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
            self.evolveCounter += 1
            # roll to A+B at T+2
            if self.evolveCounter == 2:
                self.lastPos = {'Fund': 0, 'A': self.sttPos['Fund'] * self.weight['AWeight'], 'B': self.sttPos['Fund'] * self.weight['BWeight']}
            # need to consider position change from downfold/upfold after spliting to A and B
            if px['BValue'] - self.lastpx['BValue'] > 0.2 and self.evolveCounter >= 2:
                # downfold happened
                if self.downfolded or self.upfolded:
                    raise "ticker %s, pos stt %s, date %s, multiple fold happen" % ('/'.join(self.Ticker), self.startDate.strftime("%Y%m%d"), date.strftime("%Y%m%d")) 
                self.downfolded = True
                ashare          = self.lastPos['A']
                bshare          = self.lastPos['B']
                fshare          = ashare * abs(self.lastpx['AValue'] - self.lastpx['BValue'])
                self.lastPos = {'Fund': fshare, 'A': ashare * self.lastpx['BValue'], 'B': bshare * self.lastpx['BValue']}
            if px['BValue'] - self.lastpx['BValue'] < -0.2 and self.evolveCounter >= 2:
                # upfold happened
                if self.downfolded or self.upfolded:
                    raise "ticker %s, pos stt %s, date %s, multiple fold happen" % ('/'.join(self.Ticker), self.startDate.strftime("%Y%m%d"), date.strftime("%Y%m%d")) 
                self.upfolded   = True
                ashare          = self.lastPos['A']
                bshare          = self.lastPos['B']
                fshare          = ashare * self.lastpx['AValue'] + bshare * self.lastpx['BValue'] - ashare - bshare
                self.lastPos = {'Fund': fshare, 'A': ashare, 'B': bshare}
            
            self.lastpx   = dict(px)
            self.lastDate = date
            fval          = px['FundValue'] if self.notExchTraded else px['FundPrice']
            self.lastMTM  = self.lastPos['Fund'] * fval * (1 - self.unwindRate) + (self.lastPos['A'] * px['APrice'] + self.lastPos['B'] * px['BPrice']) * (1 - self.sellFeeRate)
            if self.evolveCounter >= 3:
                self.unwinded = True
                self.endDate  = date
                return (0., self.lastMTM)
            else:
                return (self.lastMTM, 0.)  
        else:
            return (self.lastMTM, 0.)
            
    def getSummary(self):
        self.totalpnl  = self.lastMTM - self.sttCost
        self.estpnl    = (self.sttPos['A'] * self.sttpx['APrice'] + self.sttPos['B'] * self.sttpx['BPrice']) * (1 - self.sellFeeRate)
        self.mktpnl    = self.totalpnl - self.estpnl
        return super(ABMergePos, self).getSummary()
        